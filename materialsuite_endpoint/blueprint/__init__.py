import tempfile
from pathlib import Path
from uuid import uuid4
from os import makedirs, scandir
from datetime import datetime
from hashlib import md5 as _md5
import logging

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from flask import Blueprint, abort, send_file
from flask_restful import Resource, Api, reqparse

from pypremis.lib import PremisRecord
from pypremis.nodes import Event, EventDetailInformation, EventIdentifier
from pypremis.factories import LinkingObjectIdentifierFactory, \
    LinkingEventIdentifierFactory
from pypairtree.utils import identifier_to_path as id_to_path
from pypairtree.utils import path_to_identifier as path_to_id

BLUEPRINT = Blueprint('materialsuite_endpoint', __name__)


BLUEPRINT.config = {
    'LTS_PATH': str(Path(tempfile.gettempdir(), 'lts')),
    'PREMIS_PATH': str(Path(tempfile.gettempdir(), 'premis')),
    'BUFF': 65536
}


API = Api(BLUEPRINT)


log = logging.getLogger(__name__)

# This is HEAVILY linked to disk read time, and so probably won't scale well
def get_ids(root):
    def get_files(path):
        for entry in scandir(path):
            if entry.is_file():
                yield entry.path
            elif entry.is_dir():
                yield from get_files(entry.path)

    return [path_to_id(Path(x).relative_to(root).parent.parent) for x in
            get_files(root)]


def check_limit(x):
    if x > BLUEPRINT.config.get("MAX_LIMIT", 1000):
        return BLUEPRINT.config.get("MAX_LIMIT", 1000)
    return x


class Root(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("offset", type=int, default=0)
        parser.add_argument("limit", type=int, default=1000)
        args = parser.parse_args()
        args['limit'] = check_limit(args['limit'])
        paginated_ids = sorted(get_ids(BLUEPRINT.config['LTS_PATH']))[args['offset']:args['offset']+args['limit']]
        return {"materialsuites": [{"identifier": x, "_link": API.url_for(MaterialSuite, id=x)} for x
                                   in paginated_ids]}

class MaterialSuite(Resource):
    def get(self, id):
        log.info("GET received @ MaterialSuite endpoint")
        if Path(BLUEPRINT.config['LTS_PATH'], id_to_path(id), "arf").is_dir():
            log.debug("Found MaterialSuite with id: {}".format(id))
            return {"premis": API.url_for(MaterialSuitePREMIS, id=id),
                    "content": API.url_for(MaterialSuiteContent, id=id),
                    "_self": API.url_for(MaterialSuite, id=id)}
        log.debug("No MaterialSuite found with id: {}".format(id))

    # nuclear delete?
    def delete(self, id):
        raise NotImplementedError()


class MaterialSuiteContent(Resource):
    def get(self, id):
        log.info("GET received @ MaterialSuiteContent endpoint")
        req_path = Path(BLUEPRINT.config['LTS_PATH'], id_to_path(id), "arf",
                        "content.file")
        if req_path.is_file():
            log.debug("Content found for MaterialSuite with id: {}".format(
                id))
            return send_file(str(req_path))
        log.debug("No content found for MaterialSuite with id: {}".format(
            id))

    # de-accession
    def delete(self, id):
        pass


class MaterialSuitePREMIS(Resource):
    def get(self, id):
        log.info("GET received @ MaterialSuitePremis endpoint")
        req_path = Path(BLUEPRINT.config['PREMIS_PATH'], id_to_path(id), "arf",
                        "premis.xml")
        if req_path.is_file():
            log.debug("Premis found for MaterialSuite with id: {}".format(
                id))
            return send_file(str(req_path))
        log.debug("No premis found for MaterialSuite with id: {}".format(
            id))

    def put(self, id):
        # TODO
        # When scaling this microservice this functionality introduces a race
        # condition if the premis environment is shared amongst nodes.
        log.info("PUT received @ MaterialSuitePremis endpoint")
        log.warn("THIS ENDPOINT CURRENTLY INTRODUCES A RACE CONDITION IF " +
                 "THIS SERVICE IS SCALED OR RUNNING MULTITHREADED")
        log.debug("Parsing arguments")
        parser = reqparse.RequestParser()
        parser.add_argument(
            "premis",
            help="The updated PREMIS file",
            required=True,
            type=FileStorage,
            location="files"
        )
        args = parser.parse_args()
        log.debug("Arguments parsed")

        rec_path = Path(BLUEPRINT.config['PREMIS_PATH'], id_to_path(id), "arf",
                        "premis.xml")
        if not rec_path.is_file():
            # you can't use this to create PREMIS records, only update them
            log.critical("No PREMIS found for {}".format(id))
            abort(404)
        log.debug("Saving PREMIS file")
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
                e = Event(_build_eventIdentifier(),
                          "ingestion", datetime.now().isoformat())
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

        log.info("POST received @ AddMaterialSuite endpoint")
        log.debug("Parsing arguments")
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
        log.debug("Arguments parsed")

        premis_rec = None
        log.debug("Instantiating and reading PREMIS")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_premis_path = str(Path(tmpdir, uuid4().hex))
            args['premis'].save(tmp_premis_path)
            premis_rec = PremisRecord(frompath=tmp_premis_path)
        log.debug("Getting the identifier")
        identifier = premis_rec.get_object_list()[0].\
            get_objectIdentifier()[0].\
            get_objectIdentifierValue()
        if identifier != secure_filename(identifier):
            log.critical(
                "Insecure identifier detected! ({})".format(identifier)
            )
            abort(500)
        else:
            log.debug("Identifier Found: {}".format(identifier))

        content_dir = str(Path(BLUEPRINT.config['LTS_PATH'],
                               id_to_path(identifier),
                               "arf"))
        premis_dir = str(Path(BLUEPRINT.config['PREMIS_PATH'],
                              id_to_path(identifier),
                              "arf"))
        log.debug("Creating containing dirs")
        makedirs(content_dir)
        makedirs(premis_dir)

        target_content_path = str(Path(content_dir, "content.file"))
        target_premis_path = str(Path(premis_dir, "premis.xml"))
        if Path(target_content_path).exists() or \
                Path(target_premis_path).exists():
            # Never clobber stuff
            abort(500)

        log.debug("Saving content")
        args['content'].save(target_content_path)
        log.debug("Content saved")
        log.debug("Calculating md5 of file")
        md5 = None
        with open(str(target_content_path), "rb") as f:
            hasher = _md5()
            data = f.read(BLUEPRINT.config['BUFF'])
            while data:
                hasher.update(data)
                data = f.read(BLUEPRINT.config['BUFF'])
            md5 = hasher.hexdigest()
        log.debug("md5 of file calculated: {}".format(md5))
        log.debug("Retrieving md5 from PREMIS")
        premis_md5 = get_md5_from_premis(premis_rec)
        log.debug("PREMIS md5 retrieved: {}".format(premis_md5))
        if md5 != premis_md5:
            log.critical("PREMIS md5 and calculated md5 do not match!")
            abort(500)
        else:
            log.debug("Calculated md5 and PREMIS md5 match")
        log.debug("Adding ingest event to PREMIS record")
        add_ingest_event(premis_rec)
        log.debug("Ingest event added")
        log.debug("Writing PREMIS to file")
        premis_rec.write_to_file(target_premis_path)
        log.debug("PREMIS written")
        return {"created": API.url_for(MaterialSuite, id=identifier)}


@BLUEPRINT.record
def handle_configs(setup_state):
    app = setup_state.app
    BLUEPRINT.config.update(app.config)
    if BLUEPRINT.config.get("TEMPDIR"):
                tempfile.tempdir = BLUEPRINT.config['TEMPDIR']
    if BLUEPRINT.config.get("VERBOSITY"):
        logging.basicConfig(level=BLUEPRINT.config['VERBOSITY'])
    else:
        logging.basicConfig(level="WARN")

API.add_resource(Root, "/")
API.add_resource(AddMaterialSuite, "/add")
API.add_resource(MaterialSuite, "/<string:id>")
API.add_resource(MaterialSuiteContent, "/<string:id>/content")
API.add_resource(MaterialSuitePREMIS, "/<string:id>/premis")
