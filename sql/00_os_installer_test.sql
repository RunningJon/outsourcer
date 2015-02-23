CREATE EXTERNAL WEB TABLE os_installer_test
(foo int)
EXECUTE :EXECUTE ON HOST FORMAT 'TEXT' (DELIMITER '|' null 'null');
