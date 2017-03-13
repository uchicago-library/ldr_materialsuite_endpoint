import tempfile
from pathlib import Path
from uuid import uuid4
from datetime import datetime
from hashlib import md5 as _md5
from json import loads
import logging
from xml.etree.ElementTree import fromstring
from xml.etree.ElementTree import tostring
from xml.etree.ElementTree import ElementTree as ETree
from os.path import join
from abc import ABCMeta, abstractmethod

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from flask import Blueprint, abort, send_file, Response
from flask_restful import Resource, Api, reqparse

from pymongo import MongoClient, ASCENDING
from gridfs import GridFS

from xmljson import GData

from pypremis.lib import PremisRecord
from pypremis.nodes import Event, EventDetailInformation, EventIdentifier
from pypremis.factories import LinkingObjectIdentifierFactory, \
    LinkingEventIdentifierFactory

BLUEPRINT = Blueprint('materialsuite_endpoint', __name__)

GData = GData()

BLUEPRINT.config = {
    'MONGO_LTS_HOST': None,
    'MONGO_PREMIS_HOST': None,
    'MONGO_LTS_PORT': 27017,
    'MONGO_PREMIS_PORT': 27017,
    'MONGO_LTS_DB': 'lts',
    'MONGO_PREMIS_DB': 'premis',
    '_LTS_FS': None,
    '_PREMIS_DB': None
}


API = Api(BLUEPRINT)


log = logging.getLogger(__name__)


def output_xml(data, code, headers=None):
    # https://github.com/flask-restful/flask-restful/issues/124
    resp = Response(data, mimetype='text/xml', headers=headers)
    resp.status_code = code
    return resp


class IStorageBackend(metaclass=ABCMeta):
    @abstractmethod
    def get_materialsuite_id_list(self, offset, limit):
        pass

    @abstractmethod
    def check_materialsuite_exists(self, id):
        pass

    @abstractmethod
    def get_materialsuite_content(self, id):
        # In: str
        # Out: File like object
        pass

    @abstractmethod
    def set_materialsuite_content(self, id, content):
        # In: str + flask.FileStorage
        # Out: None
        pass

    @abstractmethod
    def get_materialsuite_premis(self, id):
        # In: str
        # Out: PremisRecord
        pass

    @abstractmethod
    def set_materialsuite_premis(self, id, premis):
        # In: str + PremisRecord
        # Out: None
        pass

    @abstractmethod
    def diff_materialsuite_premis(self, id, diff):
        pass

    def get_materialsuite_premis_json(self, id):
        return GData.data(self.get_materialsuite_premis(id).to_tree().getroot())


class MongoStorageBackend(IStorageBackend):
    @staticmethod
    def escape(text):
        a = text.replace("~", "~~")
        b = a.replace(".", "~p")
        c = b.replace("$", "~d")
        return c

    @staticmethod
    def unescape(text):
        a = text.replace("~d", "$")
        b = a.replace("~p", ".")
        c = b.replace("~~", "~")
        return c

    @classmethod
    def change_keys(cls, obj, convert):
        # http://stackoverflow.com/a/38269945
        """
        Recursively goes through the dictionary obj and replaces keys with the
        convert function.
        """
        if isinstance(obj, (str, int, float)):
            return obj
        if isinstance(obj, dict):
            new = obj.__class__()
            for k, v in obj.items():
                new[convert(k)] = cls.change_keys(v, convert)
        elif isinstance(obj, (list, set, tuple)):
            new = obj.__class__(cls.change_keys(v, convert) for v in obj)
        else:
            return obj
        return new

    @classmethod
    def mongo_escape(cls, some_dict):
        return cls.change_keys(some_dict, cls.escape)

    @classmethod
    def mongo_unescape(cls, some_dict):
        return cls.change_keys(some_dict, cls.unescape)

    def __init__(self, content_db_host, content_db_port, content_db_name,
                 premis_db_host, premis_db_port, premis_db_name):
        self.content_fs = GridFS(
            MongoClient(content_db_host, content_db_port)[content_db_name]
        )
        self.premis_db = MongoClient(premis_db_host, premis_db_port)[premis_db_name].records

    def get_materialsuite_id_list(self, offset, limit):
        return self.premis_db.find().sort('_id', ASCENDING).skip(offset).limit(limit)

    def check_materialsuite_exists(self, id):
        if self.premis_db.find_one({"_id": id}):
            return True
        return False

    def get_materialsuite_content(self, id):
        gr_entry = self.content_fs.find_one({"_id": id})
        return gr_entry

    def check_materialsuite_content_exists(self, id):
        if self.content_fs.find_one({"_id": id}):
            return True
        return False

    def set_materialsuite_content(self, id, content):
        if self.check_materialsuite_content_exists(id):
            raise RuntimeError("Does not support overwriting existing " +
                               "materialsuite content! Content exists for " +
                               "MaterialSuite {}".format(id))
        content_target = self.content_fs.new_file(_id=id)
        content.save(content_target)
        content_target.close()

    def get_materialsuite_premis(self, id):
        entry = self.premis_db.find_one({"_id": id})
        if entry:
            log.debug("PREMIS found for MaterialSuite with id: {}".format(
                id))
            # Convert JSON to XML
            xml_element = GData.etree(self.mongo_unescape(entry['premis_json']))[0]
            tree = ETree(xml_element)
            # We have to screw around with tempfiles because I'm lazy and
            # haven't written the functions to load things into PremisRecords
            # straight from io.
            # We use pypremis to handle the XML declaration shennanigans and
            # namespaces as well as the field order issues.
            with tempfile.TemporaryDirectory() as tmp_dir:
                fp = join(tmp_dir, uuid4().hex)
                tree.write(fp, encoding="UTF-8", xml_declaration=True,
                           method="xml")
                rec = PremisRecord(frompath=fp)
            return rec

    def check_materialsuite_premis_exists(self, id):
        if self.premis_db.find_one({"_id": id}):
            return True
        return False

    def set_materialsuite_premis(self, id, premis):
        if self.check_materialsuite_premis_exists(id):
            log.info("Overwriting PREMIS record for Materialsuite {}".format(id))
        premis_json = GData.data(premis.to_tree().getroot())
        print(premis_json)
        self.premis_db.insert_one(
            {"_id": id, "premis_json": self.mongo_escape(premis_json)}
        )

    def diff_materialsuite_premis(self, id, diff):
        raise NotImplementedError


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
                {"identifier": x['_id'], "_link": API.url_for(MaterialSuite, id=x['_id'])} for x
                in BLUEPRINT.config['storage'].get_materialsuite_id_list(args['offset'], args['limit'])
            ],
            "limit": args['limit'],
            "offset": args['offset']
        }


class MaterialSuite(Resource):
    def get(self, id):
        log.info("GET received @ MaterialSuite endpoint")
        if BLUEPRINT.config['storage'].check_materialsuite_exists(id):
            log.debug("Found MaterialSuite with id: {}".format(id))
            return {"premis": API.url_for(MaterialSuitePREMIS, id=id),
                    "content": API.url_for(MaterialSuiteContent, id=id),
                    "_self": API.url_for(MaterialSuite, id=id)}
        else:
            return {"message": "No such materialsuite"}
        log.debug("No MaterialSuite found with id: {}".format(id))

    # nuclear delete?
    def delete(self, id):
        raise NotImplementedError()


class MaterialSuiteContent(Resource):
    def get(self, id):
        log.info("GET received @ MaterialSuiteContent endpoint")
        entry = BLUEPRINT.config['storage'].get_materialsuite_content(id)
        if entry:
            log.debug("Content found for MaterialSuite with id: {}".format(
                id))
            # TODO - get the mime from the premis and try it?
            return send_file(entry, mimetype="application/octet-stream")
        log.debug("No content found for MaterialSuite with id: {}".format(
            id))

    # de-accession
    def delete(self, id):
        pass


class MaterialSuitePREMIS(Resource):
    def get(self, id):
        log.info("GET received @ MaterialSuitePremis endpoint")
        if BLUEPRINT.config['storage'].check_materialsuite_premis_exists(id):
            premis = BLUEPRINT.config['storage'].get_materialsuite_premis(id)
            return output_xml(tostring(premis.to_tree().getroot(), encoding="unicode"), 200)

        log.debug("No premis found for MaterialSuite with id: {}".format(
            id))
        abort(404)

    def put(self, id):
        # TODO
        pass

class MaterialSuitePREMISJson(Resource):
    def get(self, id):
        log.info("GET received @ MaterialSuitePremisJson endpoint")
        premis = BLUEPRINT.config['storage'].get_materialsuite_premis_json(id)
        return premis

#    def put(self, id):
#        log.info("PUT received @ MaterialSuitePremisJson endpoint")
#        parser = reqparse.ArgumentParser()
#        parser.add_argument("premis_json", type=str)
#        args = parser.parse_args()
#
#        BLUEPRINT.config['_PREMIS_DB'].insert_one(
#            {"_id": id, "record": loads(args['premis_json'])}
#        )
#
#    def patch(self, id):
#        pass


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

        log.debug("Saving content")
        BLUEPRINT.config['storage'].set_materialsuite_content(identifier, args['content'])
        log.debug("Content saved")
        log.debug("Adding ingest event to PREMIS record")
        add_ingest_event(premis_rec)
        log.debug("Ingest event added")
        # TODO: Add fixity check (via interface?) to newly ingested materials
        # here. Updating PREMIS accordingly.
        log.debug("Writing PREMIS to tmp disk")
        BLUEPRINT.config['storage'].set_materialsuite_premis(identifier, premis_rec)
        log.debug("PREMIS written")
        return {"created": API.url_for(MaterialSuite, id=identifier)}


@BLUEPRINT.record
def handle_configs(setup_state):
    app = setup_state.app
    BLUEPRINT.config.update(app.config)

    BLUEPRINT.config['storage'] = MongoStorageBackend(
        "mongo", 27017, "lts",
        "mongo", 27017, "premis"
    )

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
API.add_resource(MaterialSuitePREMISJson, "/<string:id>/premis/json")
