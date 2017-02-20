import tempfile
from pathlib import Path
from uuid import uuid4
from os import makedirs
from datetime import datetime
from hashlib import md5 as _md5

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from flask import Blueprint, abort, send_file
from flask_restful import Resource, Api, reqparse

from pypremis.lib import PremisRecord
from pypremis.nodes import Event, EventDetailInformation, EventIdentifier
from pypremis.factories import LinkingObjectIdentifierFactory, \
    LinkingEventIdentifierFactory
from pypairtree.utils import identifier_to_path as id_to_path

BLUEPRINT = Blueprint('materialsuite_endpoint', __name__)


BLUEPRINT.config = {
    'LTS_PATH': str(Path(tempfile.gettempdir(), 'lts')),
    'PREMIS_PATH': str(Path(tempfile.gettempdir(), 'premis')),
    'BUFF': 65536
}


API = Api(BLUEPRINT)


class MaterialSuite(Resource):
    def get(self, id):
        if Path(BLUEPRINT.config['LTS_PATH'], id_to_path(id), "arf").is_dir():
            return {"premis": API.url_for(MaterialSuitePREMIS, id=id),
                    "content": API.url_for(MaterialSuiteContent, id=id),
                    "_self": API.url_for(MaterialSuite, id=id)}

    # nuclear delete?
    def delete(self, id):
        raise NotImplementedError()


class MaterialSuiteContent(Resource):
    def get(self, id):
        req_path = Path(BLUEPRINT.config['LTS_PATH'], id_to_path(id), "arf",
                        "content.file")
        if req_path.is_file():
            return send_file(str(req_path))

    # de-accession
    def delete(self, id):
        pass


class MaterialSuitePREMIS(Resource):
    def get(self, id):
        req_path = Path(BLUEPRINT.config['PREMIS_PATH'], id_to_path(id), "arf",
                        "premis.xml")
        if req_path.is_file():
            return send_file(str(req_path))

    def put(self, id):
        parser = reqparse.RequestParser()
        parser.add_argument(
            "premis",
            help="The updated PREMIS file",
            required=True,
            type=FileStorage,
            location="files"
        )
        args = parser.parse_args()

        rec_path = Path(BLUEPRINT.config['PREMIS_PATH'], id_to_path(id), "arf",
                        "premis.xml")
        if not rec_path.is_file():
            # you can't use this to create PREMIS records, only update them
            abort(404)
        args['premis'].save(str(rec_path))
        return {"_self": API.url_for(MaterialSuitePREMIS, id=id)}


# RPC-like
class AddMaterialSuite(Resource):
    def post(self):

        def add_ingest_event(rec):
            def _build_eventDetailInformation():
                return EventDetailInformation(
                    eventDetail="bystream copied into " +
                    "the long term storage environment."
                )

            def _build_eventIdentifier():
                return EventIdentifier("uuid4", uuid4().hex)

            def _build_event():
                e = Event(_build_eventIdentifier(), "ingestion", datetime.now().isoformat())
                e.add_eventDetailInformation(_build_eventDetailInformation())
                return e

            event = _build_event()
            obj = rec.get_object_list()[0]
            event.add_linkingObjectIdentifier(
                LinkingObjectIdentifierFactory(obj).produce_linking_node()
            )
            obj.add_linkingEventIdentifier(
                LinkingEventIdentifierFactory(event).produce_linking_node()
            )
            rec.add_event(event)

        def get_md5_from_premis(rec):
            obj = rec.get_object_list()[0]
            for objChar in obj.get_objectCharacteristics():
                for fixity in objChar.get_fixity():
                    if fixity.get_messageDigestAlgorithm() == "md5":
                        return fixity.get_messageDigest()

        parser = reqparse.RequestParser()
        parser.add_argument(
            "content",
            help="Specify the content file",
            type=FileStorage,
            location='files',
            required=True
        )
        parser.add_argument(
            "premis",
            help="Specify the PREMIS file",
            type=FileStorage,
            location='files',
            required=True
        )
        args = parser.parse_args()

        premis_rec = None
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_premis_path = str(Path(tmpdir, uuid4().hex))
            args['premis'].save(tmp_premis_path)
            premis_rec = PremisRecord(frompath=tmp_premis_path)
        identifier = premis_rec.get_object_list()[0].\
            get_objectIdentifier()[0].\
            get_objectIdentifierValue()
        assert(identifier == secure_filename(identifier))

        content_dir = str(Path(BLUEPRINT.config['LTS_PATH'],
                               id_to_path(identifier),
                               "arf"))
        premis_dir = str(Path(BLUEPRINT.config['PREMIS_PATH'],
                              id_to_path(identifier),
                              "arf"))
        makedirs(content_dir)
        makedirs(premis_dir)

        target_content_path = str(Path(content_dir, "content.file"))

        args['content'].save(target_content_path)
        md5 = None
        with open(str(target_content_path), "rb") as f:
            hasher = _md5()
            data = f.read(BLUEPRINT.config['BUFF'])
            while data:
                hasher.update(data)
                data = f.read(BLUEPRINT.config['BUFF'])
            md5 = hasher.hexdigest()
        premis_md5 = get_md5_from_premis(premis_rec)
        assert(md5 == premis_md5)
        add_ingest_event(premis_rec)
        target_premis_path = str(Path(premis_dir, "premis.xml"))
        premis_rec.write_to_file(target_premis_path)
        return {"created": API.url_for(MaterialSuite, id=identifier)}


@BLUEPRINT.record
def handle_configs(setup_state):
    app = setup_state.app
    BLUEPRINT.config.update(app.config)
    if BLUEPRINT.config.get("TEMPDIR"):
                tempfile.tempdir = BLUEPRINT.config['TEMPDIR']

API.add_resource(AddMaterialSuite, "/add")
API.add_resource(MaterialSuite, "/<string:id>")
API.add_resource(MaterialSuiteContent, "/<string:id>/content")
API.add_resource(MaterialSuitePREMIS, "/<string:id>/premis")
