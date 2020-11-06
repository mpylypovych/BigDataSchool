CREATE    LOGIN max_pylypovych
WITH    PASSWORD = 'REMOVED'

CREATE SCHEMA max_pylypovych_schema
CREATE USER max_pylypovych FOR LOGIN max_pylypovych

--ALTER ROLE db_owner ADD MEMBER max_pylypovych
sp_addrolemember 'db_owner', 'max_pylypovych'

