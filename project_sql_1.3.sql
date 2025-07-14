CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f (
    from_date DATE,
    to_date DATE,
    chapter CHAR(1),
    ledger_account CHAR(5),
    characteristic CHAR(1),
    
    balance_in_rub NUMERIC(23,8),
    balance_in_val NUMERIC(23,8),
    balance_in_total NUMERIC(23,8),
    
    turn_deb_rub NUMERIC(23,8),
    turn_deb_val NUMERIC(23,8),
    turn_deb_total NUMERIC(23,8),
    
    turn_cre_rub NUMERIC(23,8),
    turn_cre_val NUMERIC(23,8),
    turn_cre_total NUMERIC(23,8),
    
    balance_out_rub NUMERIC(23,8),
    balance_out_val NUMERIC(23,8),
    balance_out_total NUMERIC(23,8)
);

CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start TIMESTAMP := clock_timestamp();
    v_end TIMESTAMP;
    v_from DATE := (i_OnDate - INTERVAL '1 MONTH')::DATE;
    v_to DATE := (i_OnDate - INTERVAL '1 DAY')::DATE;
BEGIN
    DELETE FROM dm.dm_f101_round_f
    WHERE from_date = v_from AND to_date = v_to;

    INSERT INTO dm.dm_f101_round_f (
        from_date, to_date, chapter, ledger_account, characteristic,
        balance_in_rub, balance_in_val, balance_in_total,
        turn_deb_rub, turn_deb_val, turn_deb_total,
        turn_cre_rub, turn_cre_val, turn_cre_total,
        balance_out_rub, balance_out_val, balance_out_total
    )
    SELECT
        v_from, v_to,
        led.chapter,
        LEFT(acc.account_number, 5) AS ledger_account,
        acc.char_type,

        SUM(CASE WHEN acc.currency_code IN ('643','810') THEN b_in.balance_out_rub ELSE 0 END) AS balance_in_rub,
        SUM(CASE WHEN acc.currency_code NOT IN ('643','810') THEN b_in.balance_out_rub ELSE 0 END) AS balance_in_val,
        SUM(COALESCE(b_in.balance_out_rub, 0)) AS balance_in_total,

        SUM(CASE WHEN acc.currency_code IN ('643','810') THEN t.turn_deb_rub ELSE 0 END) AS turn_deb_rub,
        SUM(CASE WHEN acc.currency_code NOT IN ('643','810') THEN t.turn_deb_rub ELSE 0 END) AS turn_deb_val,
        SUM(COALESCE(t.turn_deb_rub, 0)) AS turn_deb_total,

        SUM(CASE WHEN acc.currency_code IN ('643','810') THEN t.turn_cre_rub ELSE 0 END) AS turn_cre_rub,
        SUM(CASE WHEN acc.currency_code NOT IN ('643','810') THEN t.turn_cre_rub ELSE 0 END) AS turn_cre_val,
        SUM(COALESCE(t.turn_cre_rub, 0)) AS turn_cre_total,

        SUM(CASE WHEN acc.currency_code IN ('643','810') THEN b_out.balance_out_rub ELSE 0 END) AS balance_out_rub,
        SUM(CASE WHEN acc.currency_code NOT IN ('643','810') THEN b_out.balance_out_rub ELSE 0 END) AS balance_out_val,
        SUM(COALESCE(b_out.balance_out_rub, 0)) AS balance_out_total

    FROM ds.md_account_d acc
    JOIN ds.md_ledger_account_s led
        ON LEFT(acc.account_number, 5) = LPAD(led.ledger_account::text, 5, '0')
       AND v_from BETWEEN led.start_date AND COALESCE(led.end_date, v_from)
    LEFT JOIN dm.dm_account_balance_f b_in
        ON acc.account_rk = b_in.account_rk AND b_in.on_date = (v_from - INTERVAL '1 day')::DATE
    LEFT JOIN (
        SELECT account_rk,
            SUM(debet_amount_rub) AS turn_deb_rub,
            SUM(credit_amount_rub) AS turn_cre_rub
        FROM dm.dm_account_turnover_f
        WHERE on_date BETWEEN v_from AND v_to
        GROUP BY account_rk
    ) t ON acc.account_rk = t.account_rk
    LEFT JOIN dm.dm_account_balance_f b_out
        ON acc.account_rk = b_out.account_rk AND b_out.on_date = v_to

    WHERE acc.data_actual_date <= v_to AND acc.data_actual_end_date >= v_from
    GROUP BY
        led.chapter,
        LEFT(acc.account_number, 5),
        acc.char_type;

    v_end := clock_timestamp();
    INSERT INTO logs.etl_log(table_name, start_time, end_time, row_count, status, message)
    VALUES ('dm.dm_f101_round_f', v_start, v_end,
            (SELECT COUNT(*) FROM dm.dm_f101_round_f WHERE from_date = v_from AND to_date = v_to),
            'SUCCESS', 'Расчет формы 101');

END;
$$;

CALL dm.fill_f101_round_f('2018-02-01');




