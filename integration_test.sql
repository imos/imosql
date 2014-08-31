CREATE TABLE IF NOT EXISTS `test` (
  `test_id` int(11) NOT NULL AUTO_INCREMENT,
  `test_string` varchar(32) NOT NULL,
  `test_int` int(11) NOT NULL,
  `test_time` datetime NOT NULL,
  PRIMARY KEY (`test_id`)
) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=4;

INSERT INTO `test` (`test_id`, `test_string`, `test_int`, `test_time`) VALUES
(1, 'foo', 1, '2000-01-01 00:00:00'),
(2, 'bar', 2, '2001-02-03 04:05:06'),
(3, 'foobar', 3, '0000-01-01 00:00:00');
