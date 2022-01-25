import re

from attr import Attribute

class UUIDParser():
    def __init__(self):
        pass

    def convert_to_uuid(self, uuid):
        try:
            regexp_search = re.search(r'(\w{8})\-?(\w{4})\-?(\w{4})\-?(\w{4})\-?(\w{12})', uuid.lower(), re.IGNORECASE)
            result = f"{regexp_search.group(1)}-{regexp_search.group(2)}-{regexp_search.group(3)}-{regexp_search.group(4)}-{regexp_search.group(5)}"
        except Exception as e:
            raise(e)
        
        return result

    def convert_to_uuid_without_seperators(self, uuid):
        try:
            regexp_search = re.search(r'(\w{8})\-?(\w{4})\-?(\w{4})\-?(\w{4})\-?(\w{12})', uuid.lower(), re.IGNORECASE)
            result = f"{regexp_search.group(1)}{regexp_search.group(2)}{regexp_search.group(3)}{regexp_search.group(4)}{regexp_search.group(5)}"
        except AttributeError as e:
            raise(e)
        
        return result

    def convert_to_uppercase_uuid(self, uuid):
        try:
            regexp_search = re.search(r'(\w{8})\-?(\w{4})\-?(\w{4})\-?(\w{4})\-?(\w{12})', uuid.upper(), re.IGNORECASE)
            result = f"{regexp_search.group(1)}-{regexp_search.group(2)}-{regexp_search.group(3)}-{regexp_search.group(4)}-{regexp_search.group(5)}"
        except Exception as e:
            raise(e)
        
        return result


    def convert_to_uppercase_uuid_without_seperators(self, uuid):
        try:
            regexp_search = re.search(r'(\w{8})\-?(\w{4})\-?(\w{4})\-?(\w{4})\-?(\w{12})', uuid.upper(), re.IGNORECASE)
            result = f"{regexp_search.group(1)}{regexp_search.group(2)}{regexp_search.group(3)}{regexp_search.group(4)}{regexp_search.group(5)}"
        except Exception as e:
            raise(e)
        
        return result

    

