-- 1. Человекочитаемый TransferredPoints: Peer1, Peer2, PointsAmount
CREATE OR REPLACE FUNCTION fnc_transferred_points()
RETURNS TABLE (Peer1 VARCHAR, Peer2 VARCHAR, PointsAmount INT)
AS $$
BEGIN
    RETURN QUERY
    SELECT tp.Peer, tp.TargetPeer, SUM(tp.PointsAmount)::INT
    FROM TransferredPoints tp
    GROUP BY tp.Peer, tp.TargetPeer
    ORDER BY tp.Peer, tp.TargetPeer;
END;
$$ LANGUAGE plpgsql;

-- 2. Таблица: Peer, Task, XP
CREATE OR REPLACE FUNCTION fnc_peer_task_xp()
RETURNS TABLE (Peer VARCHAR, Task VARCHAR, XP INT)
AS $$
BEGIN
    RETURN QUERY
    SELECT c.Peer, c.Task, x.XPAmount
    FROM XP x
    JOIN Checks c ON x.CheckID = c.ID
    ORDER BY c.Peer, c.Task;
END;
$$ LANGUAGE plpgsql;

-- 3. Пиры, не покидавшие кампус весь день
CREATE OR REPLACE FUNCTION fnc_peers_in_campus_all_day(check_date DATE)
RETURNS TABLE (Peer VARCHAR)
AS $$
BEGIN
    RETURN QUERY
    SELECT t1.Peer
    FROM TimeTracking t1
    WHERE t1.Date = check_date AND t1.State = '1'
    AND NOT EXISTS (
        SELECT 1
        FROM TimeTracking t2
        WHERE t2.Peer = t1.Peer
          AND t2.Date = check_date
          AND t2.State = '2'
    )
    GROUP BY t1.Peer;
END;
$$ LANGUAGE plpgsql;

-- 4. Изменение баллов каждого пира (разница: получено - передано)
CREATE OR REPLACE FUNCTION fnc_peer_points_change()
RETURNS TABLE (Peer VARCHAR, PointsChange BIGINT)
AS $$
BEGIN
    RETURN QUERY
    WITH PointsIn AS (
        SELECT TargetPeer AS Peer, SUM(PointsAmount) AS Points
        FROM TransferredPoints
        GROUP BY TargetPeer
    ),
    PointsOut AS (
        SELECT Peer, SUM(PointsAmount) AS Points
        FROM TransferredPoints
        GROUP BY Peer
    )
    SELECT
        COALESCE(p_in.Peer, p_out.Peer) AS Peer,
        (COALESCE(p_in.Points, 0) - COALESCE(p_out.Points, 0)) AS PointsChange
    FROM PointsIn p_in
    FULL OUTER JOIN PointsOut p_out ON p_in.Peer = p_out.Peer
    ORDER BY PointsChange DESC;
END;
$$ LANGUAGE plpgsql;

-- Дополнительно (часто требуется в Info21): самая ранняя проверка по задаче
CREATE OR REPLACE FUNCTION fnc_early_morning_check(n_hour INT DEFAULT 12)
RETURNS TABLE (Peer VARCHAR, Task VARCHAR, Date DATE)
AS $$
BEGIN
    RETURN QUERY
    SELECT c.Peer, c.Task, c.Date
    FROM Checks c
    JOIN P2P p ON c.ID = p.CheckID
    WHERE p.State = 'Start'
      AND EXTRACT(HOUR FROM p.Time) < n_hour
    ORDER BY c.Date;
END;
$$ LANGUAGE plpgsql;





-- Task 2: Changing Data
-- Процедура: добавление P2P-проверки
-- Принимает: проверяемый, проверяющий, задача, статус, время
CREATE OR REPLACE PROCEDURE add_p2p_check(
    checked_peer VARCHAR,
    checking_peer VARCHAR,
    task_title VARCHAR,
    p2p_status check_status,
    check_time TIME
)
LANGUAGE plpgsql
AS $$
DECLARE
    last_check_id BIGINT;
BEGIN
    -- Если статус Start — создаём новую запись в Checks и первую запись в P2P
    IF p2p_status = 'Start' THEN
        INSERT INTO Checks (Peer, Task, Date)
        VALUES (checked_peer, task_title, CURRENT_DATE)
        RETURNING ID INTO last_check_id;

        INSERT INTO P2P (CheckID, CheckingPeer, State, Time)
        VALUES (last_check_id, checking_peer, p2p_status, check_time);
    -- Если Success/Failure — ищем последнюю Start-проверку того же дня
    ELSE
        SELECT c.ID INTO last_check_id
        FROM Checks c
        JOIN P2P p ON c.ID = p.CheckID
        WHERE c.Peer = checked_peer
          AND c.Task = task_title
          AND c.Date = CURRENT_DATE
          AND p.CheckingPeer = checking_peer
          AND p.State = 'Start'
        ORDER BY p.ID DESC
        LIMIT 1;

        -- Если Start не найден — ошибка
        IF last_check_id IS NULL THEN
            RAISE EXCEPTION 'No matching "Start" P2P check found for this peer and task';
        END IF;

        -- Добавляем запись о завершении
        INSERT INTO P2P (CheckID, CheckingPeer, State, Time)
        VALUES (last_check_id, checking_peer, p2p_status, check_time);
    END IF;
END;
$$;

-- Процедура: добавление проверки от Verter
-- Допустима только после успешного P2P
CREATE OR REPLACE PROCEDURE add_verter_check(
    checked_peer VARCHAR,
    task_title VARCHAR,
    verter_status check_status,
    check_time TIME
)
LANGUAGE plpgsql
AS $$
DECLARE
    valid_check_id BIGINT;
BEGIN
    -- Ищем проверку, где P2P завершился с Success
    SELECT c.ID INTO valid_check_id
    FROM Checks c
    JOIN P2P p ON c.ID = p.CheckID
    WHERE c.Peer = checked_peer
      AND c.Task = task_title
      AND c.Date = CURRENT_DATE
      AND p.State = 'Success'
    ORDER BY p.ID DESC
    LIMIT 1;

    IF valid_check_id IS NULL THEN
        RAISE EXCEPTION 'No successful P2P check found; Verter check cannot be added';
    END IF;

    INSERT INTO Verter (CheckID, State, Time)
    VALUES (valid_check_id, verter_status, check_time);
END;
$$;

-- Триггерная функция: при P2P Start автоматически добавлять +1 в TransferredPoints
CREATE OR REPLACE FUNCTION fnc_trg_transfer_points_on_p2p_start()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.State = 'Start' THEN
        -- Проверяющий (CheckingPeer) передаёт 1 балл проверяемому (Peer из Checks)
        INSERT INTO TransferredPoints (Peer, TargetPeer, PointsAmount)
        VALUES (
            NEW.CheckingPeer,
            (SELECT Peer FROM Checks WHERE ID = NEW.CheckID),
            1
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер: срабатывает после вставки в P2P
CREATE TRIGGER trg_transfer_points_on_p2p_start
AFTER INSERT ON P2P
FOR EACH ROW
EXECUTE FUNCTION fnc_trg_transfer_points_on_p2p_start();

-- Триггерная функция: валидация XP при вставке
CREATE OR REPLACE FUNCTION fnc_trg_validate_xp()
RETURNS TRIGGER AS $$
DECLARE
    max_xp INT;
    p2p_success BOOLEAN;
    verter_success BOOLEAN;
BEGIN
    -- Получаем MaxXP для задачи
    SELECT t.MaxXP INTO max_xp
    FROM Checks c
    JOIN Tasks t ON c.Task = t.Title
    WHERE c.ID = NEW.CheckID;

    -- Проверяем, успешен ли P2P (последняя запись)
    SELECT (p.State = 'Success') INTO p2p_success
    FROM P2P p
    WHERE p.CheckID = NEW.CheckID
    ORDER BY p.Time DESC
    LIMIT 1;

    -- Проверяем, успешен ли Verter (если есть)
    SELECT (v.State = 'Success') INTO verter_success
    FROM Verter v
    WHERE v.CheckID = NEW.CheckID
    ORDER BY v.Time DESC
    LIMIT 1;

    -- Если Verter не проверял — считаем, что проверка прошла
    IF verter_success IS NULL THEN
        verter_success := TRUE;
    END IF;

    -- XP можно добавить только если:
    -- 1) P2P и Verter (если был) успешны
    -- 2) XP не превышает MaxXP задачи
    IF NOT (p2p_success AND verter_success) OR NEW.XPAmount > max_xp THEN
        RAISE EXCEPTION 'XP validation failed: check not successful or XP exceeds MaxXP (%)', max_xp;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер: срабатывает ДО вставки в XP
CREATE TRIGGER trg_validate_xp
BEFORE INSERT ON XP
FOR EACH ROW
EXECUTE FUNCTION fnc_trg_validate_xp();



-- Task 3: Getting Data
-- Функции для удобного извлечения и анализа данных
-- Функция: возвращает TransferredPoints в человекочитаемом виде
-- (Peer1, Peer2, суммарное количество переданных баллов)
CREATE OR REPLACE FUNCTION fnc_transferred_points()
RETURNS TABLE (Peer1 VARCHAR, Peer2 VARCHAR, PointsAmount INT)
AS $$
BEGIN
    RETURN QUERY
    SELECT tp.Peer, tp.TargetPeer, SUM(tp.PointsAmount)::INT
    FROM TransferredPoints tp
    GROUP BY tp.Peer, tp.TargetPeer
    ORDER BY tp.Peer, tp.TargetPeer;
END;
$$ LANGUAGE plpgsql;

-- Функция: таблица вида (Пир, Задача, XP)
CREATE OR REPLACE FUNCTION fnc_peer_task_xp()
RETURNS TABLE (Peer VARCHAR, Task VARCHAR, XP INT)
AS $$
BEGIN
    RETURN QUERY
    SELECT c.Peer, c.Task, x.XPAmount
    FROM XP x
    JOIN Checks c ON x.CheckID = c.ID
    ORDER BY c.Peer, c.Task;
END;
$$ LANGUAGE plpgsql;

-- Функция: найти пиров, не покидавших кампус весь день
-- (вошли, но не вышли)
CREATE OR REPLACE FUNCTION fnc_peers_in_campus_all_day(check_date DATE)
RETURNS TABLE (Peer VARCHAR)
AS $$
BEGIN
    RETURN QUERY
    SELECT t1.Peer
    FROM TimeTracking t1
    WHERE t1.Date = check_date AND t1.State = '1'  -- вошли
      AND NOT EXISTS (                             -- но не вышли
          SELECT 1
          FROM TimeTracking t2
          WHERE t2.Peer = t1.Peer
            AND t2.Date = check_date
            AND t2.State = '2'
      )
    GROUP BY t1.Peer;
END;
$$ LANGUAGE plpgsql;

-- Функция: изменение баллов каждого пира (получено - отдано)
CREATE OR REPLACE FUNCTION fnc_peer_points_change()
RETURNS TABLE (Peer VARCHAR, PointsChange BIGINT)
AS $$
BEGIN
    RETURN QUERY
    WITH
        PointsIn AS (
            SELECT TargetPeer AS Peer, SUM(PointsAmount) AS Points
            FROM TransferredPoints
            GROUP BY TargetPeer
        ),
        PointsOut AS (
            SELECT Peer, SUM(PointsAmount) AS Points
            FROM TransferredPoints
            GROUP BY Peer
        )
    SELECT
        COALESCE(p_in.Peer, p_out.Peer) AS Peer,
        (COALESCE(p_in.Points, 0) - COALESCE(p_out.Points, 0)) AS PointsChange
    FROM PointsIn p_in
    FULL OUTER JOIN PointsOut p_out ON p_in.Peer = p_out.Peer
    ORDER BY PointsChange DESC;
END;
$$ LANGUAGE plpgsql;



-- Task 4: Metadata
-- Процедуры для управления метаданными и тестирования
-- Примечание: создание тестовой БД выполняется ВНЕ этого скрипта:
--   CREATE DATABASE info21_test;
-- Процедура: удалить ВСЕ таблицы и ENUM-типы в текущей БД
CREATE OR REPLACE PROCEDURE prc_drop_all_tables()
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN
    -- Удаляем все таблицы в схеме public
    FOR r IN (
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;

    -- Удаляем пользовательские ENUM-типы
    FOR r IN (
        SELECT t.typname
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        GROUP BY t.typname
    ) LOOP
        EXECUTE 'DROP TYPE IF EXISTS ' || quote_ident(r.typname) || ' CASCADE';
    END LOOP;
END;
$$;

-- Процедура: вывести список скалярных SQL-функций пользователя (имя + параметры)
CREATE OR REPLACE PROCEDURE prc_show_user_functions()
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN
    RAISE NOTICE 'User scalar functions in current database:';
    FOR r IN (
        SELECT
            p.proname AS func_name,
            pg_get_function_identity_arguments(p.oid) AS args
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'public'
          AND p.prokind = 'f'               -- только скалярные функции
          AND p.prorettype != 'void'::regtype -- исключаем процедуры
        ORDER BY func_name
    ) LOOP
        RAISE NOTICE 'Function: % (%)', r.func_name, r.args;
    END LOOP;
END;
$$;

-- Процедура: удалить все DML-триггеры в БД
CREATE OR REPLACE PROCEDURE prc_drop_all_triggers()
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT tgname, tgrelid::regclass::text AS table_name
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = 'public'
          AND NOT t.tgisinternal  -- только пользовательские триггеры
    ) LOOP
        EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident(r.tgname) ||
                ' ON ' || r.table_name;
    END LOOP;
END;
$$;

-- Процедура: найти объекты (таблицы, функции) по подстроке в имени
-- (README.md упоминает "описания", но их нет — ищем по имени)
CREATE OR REPLACE PROCEDURE prc_find_obj_by_name(search_text TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN
    RAISE NOTICE 'Tables containing "%":', search_text;
    FOR r IN (
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename ILIKE '%' || search_text || '%'
    ) LOOP
        RAISE NOTICE 'Table: %', r.tablename;
    END LOOP;

    RAISE NOTICE 'Functions containing "%":', search_text;
    FOR r IN (
        SELECT proname
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'public'
          AND proname ILIKE '%' || search_text || '%'
    ) LOOP
        RAISE NOTICE 'Function: %', r.proname;
    END LOOP;
END;
$$;
