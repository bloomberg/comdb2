CREATE TABLE jsonify (dtms datetime, dtus datetimeus, intvym intervalym, intvds intervalds, intvdsus intervaldsus, dec decimal32)$$
INSERT INTO jsonify values ("2017-10-17T141720.649 America/New_York", "2017-10-17T141720.648885 America/New_York", "10-0", "365 00:00:00.123", "365 00:00:00.123456", "3.141593")
SELECT json_object('dtms', dtms, 'dtus', dtus, 'intvym', intvym, 'intvds', intvds, 'intvdsus', intvdsus, 'dec', dec) FROM jsonify
SELECT json_valid(json_object('dtms', dtms, 'dtus', dtus, 'intvym', intvym, 'intvds', intvds, 'intvdsus', intvdsus, 'dec', dec)) FROM jsonify
