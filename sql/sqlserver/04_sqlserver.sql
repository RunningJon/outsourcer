insert into append_test values (6, 'Jon', 'Roberts', 'http://www.greenplum.com');
insert into append_test values (7, 'Steve', 'Kerr', 'http://www.espn.com');
insert into append_test values (8, 'Barack', 'Obama', 'http://www.cnn.com');
insert into append_test values (9, 'Tom', 'Hanks', 'http://www.imdb.com');
insert into append_test values (10, 'Bubba', 'Gump', 'http://www.forest.com');

update replication_test set salary = salary*2;
delete from replication_test where id = 1;
