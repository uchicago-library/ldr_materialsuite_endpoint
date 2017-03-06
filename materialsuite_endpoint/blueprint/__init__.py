import tempfile
from pathlib import Path
from uuid import uuid4
from datetime import datetime
from hashlib import md5 as _md5
import logging

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from flask import Blueprint, abort, send_file
from flask_restful import Resource, Api, reqparse

from pymongo import MongoClient, ASCENDING
from gridfs import GridFS

from pypremis.lib import PremisRecord
from pypremis.nodes import Event, EventDetailInformation, EventIdentifier
from pypremis.factories import LinkingObjectIdentifierFactory, \
    LinkingEventIdentifierFactory

BLUEPRINT = Blueprint('materialsuite_endpoint', __name__)


BLUEPRINT.config = {
    'MONGO_LTS_HOST': None,
    'MONGO_PREMIS_HOST': None,
    'MONGO_LTS_PORT': 27017,
    'MONGO_PREMIS_PORT': 27017,
    'MONGO_LTS_DB': 'lts',
    'MONGO_PREMIS_DB': 'premis',
    'BUFF': 65536,
    '_LTS_FS': None,
    '_PREMIS_FS': None
}


API = Api(BLUEPRINT)


log = logging.getLogger(__name__)


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
        return {
            "materialsuites": [
                {"identifier": x._id, "_link": API.url_for(MaterialSuite, id=x._id)} for x
                in BLUEPRINT.config['_PREMIS_FS'].find().sort('_id', ASCENDING).skip(args['offset']).limit(args['limit'])
            ],
            "limit": args['limit'],
            "offset": args['offset']
        }


class MaterialSuite(Resource):
    def get(self, id):
        log.info("GET received @ MaterialSuite endpoint")
        if BLUEPRINT.config['_PREMIS_FS'].find_one({"_id": id}):
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
        gr_entry = BLUEPRINT.config['_LTS_FS'].find_one({"_id": id})
        if gr_entry:
            log.debug("Content found for MaterialSuite with id: {}".format(
                id))
            # TODO - get the mime from the premis and try it?
            return send_file(gr_entry, mimetype="application/octet-stream")
        log.debug("No content found for MaterialSuite with id: {}".format(
            id))

    # de-accession
    def delete(self, id):
        pass


class MaterialSuitePREMIS(Resource):
    def get(self, id):
        log.info("GET received @ MaterialSuitePremis endpoint")
        gr_entry = BLUEPRINT.config['_PREMIS_FS'].find_one({"_id": id})
        if gr_entry:
            log.debug("PREMIS found for MaterialSuite with id: {}".format(
                id))
            return send_file(gr_entry, mimetype="text/xml")

        log.debug("No premis found for MaterialSuite with id: {}".format(
            id))

    def put(self, id):
        # TODO
        pass


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

        log.debug("Creating containing dirs")

        log.debug("Saving content")
        content_target = BLUEPRINT.config['_LTS_FS'].new_file(_id=identifier)
        args['content'].save(content_target)
        content_target.close()
        log.debug("Content saved")
        log.debug("Adding ingest event to PREMIS record")
        add_ingest_event(premis_rec)
        log.debug("Ingest event added")
        log.debug("Writing PREMIS to tmp disk")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_premis_path = str(Path(tmpdir, uuid4().hex))
            premis_rec.write_to_file(tmp_premis_path)
            premis_target = BLUEPRINT.config['_PREMIS_FS'].new_file(_id=identifier)
            with open(tmp_premis_path, 'rb') as f:
                premis_target.write(f.read())
                premis_target.close()
        log.debug("PREMIS written")
        return {"created": API.url_for(MaterialSuite, id=identifier)}


@BLUEPRINT.record
def handle_configs(setup_state):
    app = setup_state.app
    BLUEPRINT.config.update(app.config)

    _lts_client = MongoClient(BLUEPRINT.config['MONGO_LTS_HOST'],
                              BLUEPRINT.config['MONGO_LTS_PORT'])
    _premis_client = MongoClient(BLUEPRINT.config['MONGO_PREMIS_HOST'],
                                 BLUEPRINT.config['MONGO_PREMIS_PORT'])

    _lts_db = _lts_client[BLUEPRINT.config['MONGO_LTS_DB']]
    _premis_db = _premis_client[BLUEPRINT.config['MONGO_PREMIS_DB']]
    BLUEPRINT.config['_LTS_FS'] = GridFS(_lts_db)
    BLUEPRINT.config['_PREMIS_FS'] = GridFS(_premis_db)

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
