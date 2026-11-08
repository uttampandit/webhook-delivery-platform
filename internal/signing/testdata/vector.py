# PUBLIC TEST-ONLY key. Never use these bytes in local runtime configuration.
# Independent fixture generator: python3 internal/signing/testdata/vector.py
import hashlib
import hmac

key = bytes(range(32))
body = '{"data":{"message":"नमस्ते 🌍"}}'.encode('utf-8')
message = b'1700000000.00000000-0000-4000-8000-000000000101.' + body
# No newline: ASCII timestamp + dot + ASCII UUID + dot + exact UTF-8 body.
assert message.hex() == '313730303030303030302e30303030303030302d303030302d343030302d383030302d3030303030303030303130312e7b2264617461223a7b226d657373616765223a22e0a4a8e0a4aee0a4b8e0a58de0a4a4e0a58720f09f8c8d227d7d'
assert hmac.new(key, message, hashlib.sha256).hexdigest() == '8842482d13bc5517d907913f8c2da95e6b81e055b501051bb18954cf14de2ca7'
print('Public fixture bytes and digest verified independently')
