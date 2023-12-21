import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.types import  StructType, StructField, StringType, FloatType, DataType, ArrayType


def get_latest_version_schema(url: str, topic: str):

    schema_registry_client = SchemaRegistryClient({'url': url})
    
    latest_version = schema_registry_client.get_latest_version(subject_name=topic+"-value").version

    schema_str = schema_registry_client.get_version(
        subject_name=topic+"-value", 
        version=latest_version
    ).schema.schema_str

    return json.loads(schema_str)


def json_schema_to_spark_structtype(json_obj):

    type_map = {
        "string": StringType,
        "number": FloatType
    }

    fields = []
    for k,v in json_obj.items():
        if k=="properties":
            for field, field_value in v.items():
                fields.append( StructField( field, type_map[field_value['type']]() ) )

    return StructType(fields)