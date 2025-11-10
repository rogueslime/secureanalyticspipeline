DELIMITER $$
CREATE PROCEDURE anl.seed_dim_date(IN p_start DATE, IN p_end DATE)
BEGIN
  DECLARE d DATE;
  SET d = p_start;
  WHILE d <= p_end DO
    INSERT INTO anl.dim_date (date_key, `date`, `year`, `month`, `day`, `dow`)
    VALUES (CAST(DATE_FORMAT(d, '%Y%m%d') AS UNSIGNED), d,
            YEAR(d), MONTH(d), DAY(d), DAYOFWEEK(d))
    ON DUPLICATE KEY UPDATE `date`=VALUES(`date`);
    SET d = DATE_ADD(d, INTERVAL 1 DAY);
  END WHILE;
END$$
DELIMITER ;

CALL anl.seed_dim_date('2024-11-01','2025-12-31');
DROP PROCEDURE anl.seed_dim_date;