import tempfile
from os.path import join
from os import makedirs
from uuid import uuid4
from datetime import datetime
from hashlib import md5 as _md5
import logging
from xml.etree.ElementTree import tostring
from xml.etree.ElementTree import ElementTree as ETree
from abc import ABCMeta, abstractmethod
from pathlib import Path

try:
    import boto3
    import botocore
except:
    # Hope we're not using the s3 backend
    pass

try:
    from pymongo import MongoClient, ASCENDING
    from gridfs import GridFS
except:
    # Hope we're not using a mongo backend
    pass

try:
    from pypairtree.utils import identifier_to_path
except:
    # Hope we're not using a file system backend
    pass

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from flask import Blueprint, abort, send_file, Response, stream_with_context
from flask_restful import Resource, Api, reqparse
from xmljson import GData

from pypremis.lib import PremisRecord
from pypremis.nodes import Event, EventDetailInformation, EventIdentifier
from pypremis.factories import LinkingObjectIdentifierFactory, \
    LinkingEventIdentifierFactory


BLUEPRINT = Blueprint('materialsuite_endpoint', __name__)


BLUEPRINT.config = {'BUFF': 1024*16}


API = Api(BLUEPRINT)


log = logging.getLogger(__name__)


GData = GData()


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
    def get_materialsuite_content(self, id):
        # In: str
        # Out: File like object
        pass

    @abstractmethod
    def check_materialsuite_content_exists(self, id):
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
    def check_materialsuite_premis_exists(self, id):
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

    def check_materialsuite_exists(self, id):
        return self.check_materialsuite_content_exists(id) or \
            self.check_materialsuite_premis_exists(id)


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

    def __init__(self, content_db_host, content_db_port=None,
                 content_db_name=None,
                 premis_db_host=None, premis_db_port=None,
                 premis_db_name=None):

        # Sensible defaults for mongo options. Use the same mongod for content
        # and PREMIS if a PREMIS specific one isn't specified.
        # NOTE: In production using the same mongod for both things  probably
        # isn't a good idea, because of RAM churning and other complicated
        # database-y things I'm no expert at.

        if content_db_port is None:
            content_db_port = 27017
        if content_db_name is None:
            content_db_name = "lts"
        if premis_db_host is None:
            premis_db_host = content_db_host
        if premis_db_port is None:
            premis_db_port = content_db_port
        if premis_db_name is None:
            premis_db_name = "premis"

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
        self.premis_db.insert_one(
            {"_id": id, "premis_json": self.mongo_escape(premis_json)}
        )

    def diff_materialsuite_premis(self, id, diff):
        raise NotImplementedError


class MongoPremisStorageBackendMixin:
    # Inheriting classes must set self.premis_db
    # See MongoStorageBackend init
    escape = MongoStorageBackend.escape
    unescape = MongoStorageBackend.unescape
    change_keys = MongoStorageBackend.change_keys
    mongo_escape = MongoStorageBackend.mongo_escape
    mongo_unescape = MongoStorageBackend.mongo_unescape
    get_materialsuite_premis = MongoStorageBackend.get_materialsuite_premis
    check_materialsuite_premis_exists = MongoStorageBackend.check_materialsuite_premis_exists
    set_materialsuite_premis = MongoStorageBackend.set_materialsuite_premis
    diff_materialsuite_premis = MongoStorageBackend.diff_materialsuite_premis

class GridFSContentStorageBackendMixin:
    # Inheriting classes must set self.content_fs
    # See MongoStorageBackend init
    escape = MongoStorageBackend.escape
    unescape = MongoStorageBackend.unescape
    change_keys = MongoStorageBackend.change_keys
    mongo_escape = MongoStorageBackend.mongo_escape
    mongo_unescape = MongoStorageBackend.mongo_unescape
    get_materialsuite_id_list = MongoStorageBackend.get_materialsuite_id_list
    check_materialsuite_exists = MongoStorageBackend.check_materialsuite_exists
    get_materialsuite_content = MongoStorageBackend.get_materialsuite_content
    check_materialsuite_content_exists = MongoStorageBackend.check_materialsuite_content_exists
    set_materialsuite_content = MongoStorageBackend.set_materialsuite_content


class FileSystemStorageBackend(IStorageBackend):
    def __init__(self, lts_root, premis_root):
        self.lts_root = Path(lts_root)
        self.premis_root = Path(premis_root)

    def get_materialsuite_id_list(self):
        raise NotImplementedError()

    def get_materialsuite_content(self, id):
        content_path = Path(
            self.lts_root, identifier_to_path(id), "arf", "content.file"
        )
        return open(str(content_path))

    def check_materialsuite_content_exists(self, id):
        content_path = Path(
            self.lts_root, identifier_to_path(id), "arf", "content.file"
        )
        return content_path.is_file()

    def set_materialsuite_content(self, id, content):
        if self.check_materialsuite_content_exists(id):
            raise ValueError()
        content_path = Path(
            self.lts_root, identifier_to_path(id), "arf", "content.file"
        )
        makedirs(str(content_path.parent), exist_ok=True)
        content.save(str(content_path))

    def get_materialsuite_premis(self, id):
        premis_path = Path(
            self.premis_root, identifier_to_path(id), "arf", "premis.xml"
        )
        rec = PremisRecord(frompath=str(premis_path))
        print(str(rec))
        return rec

    def check_materialsuite_premis_exists(self, id):
        premis_path = Path(
            self.premis_root, identifier_to_path(id), "arf", "premis.xml"
        )
        return premis_path.is_file()

    def set_materialsuite_premis(self, id, premis):
        if self.check_materialsuite_premis_exists(id):
            log.warn("overwriting PREMIS {}".format(id))
        premis_path = Path(
            self.premis_root, identifier_to_path(id), "arf", "premis.xml"
        )
        makedirs(str(premis_path.parent), exist_ok=True)
        premis.write_to_file(str(premis_path))

    def diff_materialsuite_premis(self, id, diff):
        raise NotImplementedError



class FileSystemContentStorageBackendMixin:
    # Inheriting classes must set self.lts_root
    # See FileSystemStorageBackend init
    get_materialsuite_id_list = FileSystemStorageBackend.get_materialsuite_id_list
    check_materialsuite_exists = FileSystemStorageBackend.check_materialsuite_exists
    get_materialsuite_content = FileSystemStorageBackend.get_materialsuite_content
    check_materialsuite_content_exists = FileSystemStorageBackend.check_materialsuite_content_exists
    set_materialsuite_content = FileSystemStorageBackend.set_materialsuite_content


class FileContentMongoPremisStorageBackend(
    FileSystemContentStorageBackendMixin,
    MongoPremisStorageBackendMixin,
    IStorageBackend
):
    def __init__(self, lts_root,
                 premis_db_host, premis_db_port=None, premis_db_name=None):
        if premis_db_port is None:
            premis_db_port = 27017
        if premis_db_name is None:
            premis_db_name = 'premis'

        self.lts_root = Path(lts_root)
        self.premis_db = MongoClient(premis_db_host, premis_db_port)[premis_db_name].records


class SwiftContentStorageBackendMixin:
    def get_materialsuite_id_list(self):
        pass

    def get_materialsuite_content(self, id):
        pass

    def check_materialsuite_exists(self, id):
        pass

    def check_materialsuite_content_exists(self, id):
        pass

    def set_materialsuite_content(self, id, content):
        pass


class S3ContentStorageBackendMixin:
    def s3_init(self, bucket_name, region_name=None, aws_access_key_id=None, aws_secret_access_key=None):
        # Helper init for inheriting classes.
        self.s3 = boto3.client(
            's3', region_name=region_name, aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self.bucket = bucket_name
        exists = True
        try:
            self.s3.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
        if not exists:
            # Init the bucket
            self.s3.create_bucket(Bucket=bucket_name)

    def get_materialsuite_id_list(self, offset, limit):
        # TODO: Actually use api implementations of item queries
        return self.s3.list_objects(Bucket=self.bucket)[offset:offset+limit]

    def get_materialsuite_content(self, id):
        obj = self.s3.get_object(Bucket=BLUEPRINT.config['storage'].name, Key=id)
        return obj['Body']

    def check_materialsuite_content_exists(self, id):
        try:
            self.s3.head_object(Bucket=self.bucket)
            return True
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False

    def set_materialsuite_content(self, id, content):
        if self.check_materialsuite_exists(id):
            raise ValueError()
        self.s3.Object(BLUEPRINT.config['storage'].name, id).put(Body=content)

class S3ContentMongoPremisStorageBackend(
    S3ContentStorageBackendMixin,
    MongoPremisStorageBackendMixin,
    IStorageBackend
):
    def __init__(self, bucket_name, premis_db_host,
                 region_name=None, aws_access_key_id=None, aws_secret_access_key=None,
                 premis_db_port=None, premis_db_name=None):
        # TODO
        pass


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

        def generate(e):
            data = True
            while data:
                data = e.read(BLUEPRINT.config['BUFF'])
                yield data

        log.info("GET received @ MaterialSuiteContent endpoint")
        entry = BLUEPRINT.config['storage'].get_materialsuite_content(id)
        if entry:
            log.debug("Content found for MaterialSuite with id: {}".format(
                id))
            # TODO - get the mime from the premis and try it?
#            return send_file(entry, mimetype="application/octet-stream")
            return Response(stream_with_context(generate(entry)))
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

    def init_S3ContentMongoPremis(bp):
        pass


    def init_mongo(bp):
        bp.config['storage'] = MongoStorageBackend(
            bp.config['LTS_MONGO_HOST'],
            bp.config.get('LTS_MONGO_PORT'),
            bp.config.get('LTS_MONGO_DB_NAME'),
            bp.config.get('PREMIS_MONGO_HOST'),
            bp.config.get('PREMIS_MONGO_PORT'),
            bp.config.get('PREMIS_MONGO_DB_NAME')
        )

    def init_filesystem(bp):
        bp.config['storage'] = FileSystemStorageBackend(
            bp.config['LTS_DIR'],
            bp.config['PREMIS_DIR']
        )

    def init_mix(bp):
        bp.config['storage'] = FileContentMongoPremisStorageBackend(
            bp.config['LTS_DIR'],
            bp.config['PREMIS_MONGO_HOST'],
            bp.config.get('PREMIS_MONGO_PORT'),
            bp.config.get('PREMIS_MONGO_DB_NAME')
        )

    app = setup_state.app
    BLUEPRINT.config.update(app.config)

    storage_choice = BLUEPRINT.config['STORAGE_BACKEND']
    # NOERROR for in case they want to do something tricky with the bp config
    # from the application context to hack something in
    supported_backends = {
        'mongo': init_mongo,
        'filesystem': init_filesystem,
        'mix': init_mix,
        'noerror': None
    }
    if storage_choice.lower() not in supported_backends:
        raise RuntimeError(
            "Supported storage backends include: " +
            "{}".format(", ".join(supported_backends.keys()))
        )
    elif storage_choice.lower() == 'noerror':
        pass
    else:
        supported_backends.get(storage_choice.lower())(BLUEPRINT)

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
