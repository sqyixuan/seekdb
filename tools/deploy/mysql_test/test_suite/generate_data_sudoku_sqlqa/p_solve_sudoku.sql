create table ANSWERS
(
  PUZZLE_ID       NUMBER,
  STEP_NO         NUMBER,
  ROW_ID          NUMBER,
  COL_ID          NUMBER,
  PENCIL_MARK_IND NUMBER default 0,
  ANSWER          NUMBER,
  BOX_ID          NUMBER
)
;
create index IDX_ANSWERS_01 on ANSWERS (PUZZLE_ID);
create index IDX_ANSWERS_02 on ANSWERS (BOX_ID, PUZZLE_ID);
create index IDX_ANSWERS_03 on ANSWERS (ROW_ID, PUZZLE_ID);
create index IDX_ANSWERS_04 on ANSWERS (COL_ID, PUZZLE_ID);


create table CELLS
(
  ROW_ID NUMBER,
  COL_ID NUMBER,
  BOX_ID NUMBER
)
;
create index IDX_CELLS_01 on CELLS (ROW_ID, COL_ID, BOX_ID);

create table PUZZLE_NUMBERS
(
  PUZZLE_NUMBER NUMBER 
)
;

create sequence step_no_seq;

create table COMBINATIONS
(
  SET_ID        NUMBER,
  SET_SIZE      NUMBER,
  PUZZLE_NUMBER NUMBER
)
;
create unique index IDX_COMBINATIONS_01 on COMBINATIONS (SET_ID, PUZZLE_NUMBER);
create index IDX_COMBINATIONS_02 on COMBINATIONS (SET_SIZE, PUZZLE_NUMBER, SET_ID);
create index IDX_COMBINATIONS_03 on COMBINATIONS (PUZZLE_NUMBER, SET_ID, SET_SIZE);

create table PUZZLE_LOAD
(
  PUZZLE_ID NUMBER,
  ROW_ID    NUMBER,
  COL_ID1   NUMBER,
  COL_ID2   NUMBER,
  COL_ID3   NUMBER,
  COL_ID4   NUMBER,
  COL_ID5   NUMBER,
  COL_ID6   NUMBER,
  COL_ID7   NUMBER,
  COL_ID8   NUMBER,
  COL_ID9   NUMBER
)
;
delimiter //;
CREATE OR REPLACE PROCEDURE p_solve_sudoku
(
    p_puzzle_id      NUMBER,
    p_show_workings  NUMBER := 0
) AS
    v_step_no           NUMBER;
    v_cnt               NUMBER;
    v_answer_count      NUMBER;
    v_answer_count_last NUMBER;
    v_limit_cnt         NUMBER := 0;
    v_row_count         NUMBER := 0;
    v_singles_count     NUMBER := 0;


    FUNCTION answer_count(p_puzzle_id NUMBER) RETURN NUMBER AS
        v_cnt NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO   v_cnt
        FROM   answers a
        WHERE  a.puzzle_id = p_puzzle_id
        AND    a.pencil_mark_ind = 0;

        RETURN v_cnt;
    END;


    PROCEDURE setup_candidates(p_puzzle_id NUMBER) AS
        v_box_id NUMBER;
        v_row_id NUMBER;
        v_col_id NUMBER;

        TYPE t_answers IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
        v_pencil_marks t_answers;

        CURSOR c_cells IS
            SELECT box_id,
                   row_id,
                   col_id
            FROM   cells
            MINUS
            SELECT box_id,
                   row_id,
                   col_id
            FROM   answers
            WHERE  pencil_mark_ind = 0
            AND    puzzle_id = p_puzzle_id
            ORDER  BY row_id,
                      col_id;

        CURSOR c_pencil_marks IS
            SELECT a.answer
            FROM   (SELECT puzzle_number answer
                    FROM   puzzle_numbers
                    MINUS
                    -- Answers in that row
                    SELECT answer
                    FROM   answers
                    WHERE  pencil_mark_ind = 0
                    AND    row_id = v_row_id
                    AND    puzzle_id = p_puzzle_id
                    MINUS
                    -- Answers in that column
                    SELECT answer
                    FROM   answers
                    WHERE  pencil_mark_ind = 0
                    AND    col_id = v_col_id
                    AND    puzzle_id = p_puzzle_id
                    MINUS
                    -- Answers in that box
                    SELECT answer
                    FROM   answers
                    WHERE  pencil_mark_ind = 0
                    AND    box_id = v_box_id
                    AND    puzzle_id = p_puzzle_id) a;
    BEGIN
        -- Set denormalised box_id on answer
        UPDATE answers a
        SET    box_id = (SELECT box_id
                         FROM   cells c
                         WHERE  c.row_id = a.row_id
                         AND    c.col_id = a.col_id);

        -- Determine the initial pencil marks
        FOR r_cells IN c_cells LOOP

            v_box_id := r_cells.box_id;
            v_row_id := r_cells.row_id;
            v_col_id := r_cells.col_id;

            OPEN c_pencil_marks;
            FETCH c_pencil_marks BULK COLLECT
                INTO v_pencil_marks;
            CLOSE c_pencil_marks;

            FORALL i IN 1 .. v_pencil_marks.COUNT
                INSERT INTO answers
                    (puzzle_id,
                     step_no,
                     box_id,
                     row_id,
                     col_id,
                     pencil_mark_ind,
                     answer)
                VALUES
                    (p_puzzle_id,
                     10,
                     v_box_id,
                     v_row_id,
                     v_col_id,
                     1,
                     v_pencil_marks(i));

            COMMIT;
        END LOOP;

        COMMIT;
    END;


    PROCEDURE reset_pencil_marks(p_puzzle_id NUMBER) AS
    BEGIN
        UPDATE answers
        SET    pencil_mark_ind = 1,
               step_no         = v_cnt
        WHERE  pencil_mark_ind > 0
        AND    puzzle_id = p_puzzle_id;

    END;


    PROCEDURE output_answers(p_puzzle_id NUMBER) AS
        v_row_id NUMBER;

        CURSOR c_answers IS
            SELECT c.row_id,
                   c.col_id,
                   a.answer
            FROM   cells   c,
                   answers a
            WHERE  a.puzzle_id(+) = p_puzzle_id
            AND    a.row_id(+) = c.row_id
            AND    a.col_id(+) = c.col_id
            AND    a.pencil_mark_ind(+) = 0
            ORDER  BY c.row_id,
                      c.col_id;
    BEGIN
        v_row_id := 0;
        FOR r_answers IN c_answers LOOP

            IF v_row_id != r_answers.row_id THEN
                dbms_output.put_line(' ');
                IF r_answers.row_id IN (1, 4, 7) THEN
                    dbms_output.put_line('|===========|===========|===========|');
                ELSE
                    dbms_output.put_line('|-----------|-----------|-----------|');
                END IF;
                v_row_id := r_answers.row_id;
                dbms_output.put('|');
            END IF;
            dbms_output.put(' ' || nvl(TRIM(to_char(r_answers.answer)),
                                       ' '));
            dbms_output.put(' |');
        END LOOP;
        dbms_output.put_line(' ');
        dbms_output.put_line('|===========|===========|===========|');
    END;


    PROCEDURE output_pencil_marks
    (
        p_puzzle_id NUMBER,
        p_step_no   NUMBER := 0
    ) AS
        v_row_id NUMBER;

        CURSOR c_answers IS
            SELECT c.row_id,
                   c.col_id,
                   c.puzzle_number,
                   a.answer
            FROM   (SELECT c.row_id,
                           c.col_id,
                           n.puzzle_number
                    FROM   cells          c,
                           puzzle_numbers n) c,
                   answers a
            WHERE  a.puzzle_id(+) = p_puzzle_id
            AND    a.row_id(+) = c.row_id
            AND    a.col_id(+) = c.col_id
            AND    a.step_no(+) >= nvl(p_step_no,
                                       0)
            AND    a.pencil_mark_ind(+) >=
                   decode(nvl(p_step_no,
                               0),
                           0,
                           1,
                           -1)
            AND    a.answer(+) = c.puzzle_number
            ORDER  BY c.row_id,
                      c.col_id,
                      c.puzzle_number;
    BEGIN
        dbms_output.put_line(' ');
        dbms_output.put_line('Step No = ' || nvl(p_step_no,
                                                 0));

        v_row_id := 0;
        FOR r_answers IN c_answers LOOP

            IF v_row_id != r_answers.row_id THEN
                IF r_answers.row_id IN (1, 4, 7) THEN
                    dbms_output.put_line(' |======================================|======================================|======================================|');
                ELSE
                    dbms_output.put_line(' |--------------------------------------|--------------------------------------|--------------------------------------|');
                END IF;
                v_row_id := r_answers.row_id;
            END IF;

            IF r_answers.puzzle_number = 1 THEN
                dbms_output.put(' | ');
            END IF;

            dbms_output.put(nvl(TRIM(to_char(r_answers.answer)),
                                ' ') || CASE WHEN
                            r_answers.puzzle_number = 9 THEN ' ' ELSE NULL END);

            IF r_answers.col_id = 9 AND r_answers.puzzle_number = 9 THEN
                dbms_output.put_line(' |');
            END IF;

        END LOOP;
        dbms_output.put_line(' |======================================|======================================|======================================|');
        dbms_output.put_line(' ');
    END;



    PROCEDURE set_singles_cell
    (
        p_puzzle_id     NUMBER,
        p_show_workings NUMBER,
        p_row_count     OUT NUMBER
    ) AS
        v_step_no NUMBER;
    BEGIN

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('**************************************************************************************************************************************');
            dbms_output.put_line('SINGLES CELL');
            dbms_output.put_line('**************************************************************************************************************************************');
        END IF;

        SELECT step_no_seq.NEXTVAL
        INTO   v_step_no
        FROM   dual;

        p_row_count := 0;


        UPDATE answers
        SET    pencil_mark_ind = 0,
               step_no         = v_step_no
        WHERE  pencil_mark_ind > 0
        AND    puzzle_id = p_puzzle_id
        AND    (row_id, col_id) IN
               (SELECT a.row_id,
                        a.col_id
                 FROM   answers a
                 WHERE  a.puzzle_id = p_puzzle_id
                 AND    a.pencil_mark_ind > 0
                 GROUP  BY a.row_id,
                           a.col_id
                 HAVING COUNT(*) = 1);

        p_row_count := SQL%ROWCOUNT;

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('singles: '|| p_row_count);
            output_pencil_marks(p_puzzle_id,v_step_no);
            output_pencil_marks(p_puzzle_id);
            output_answers(p_puzzle_id);
            dbms_output.put_line('--------------------------------------------------------------------------------------------------------------------------------------');
        END IF;

    END;



    PROCEDURE set_singles_box
    (
        p_puzzle_id     NUMBER,
        p_show_workings NUMBER,
        p_row_count     OUT NUMBER
    ) AS
        v_step_no NUMBER;
    BEGIN

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('**************************************************************************************************************************************');
            dbms_output.put_line('SINGLES BOX');
            dbms_output.put_line('**************************************************************************************************************************************');
        END IF;

        SELECT step_no_seq.NEXTVAL
        INTO   v_step_no
        FROM   dual;

        p_row_count := 0;

        UPDATE answers
        SET    pencil_mark_ind = 0,
               step_no         = v_step_no
        WHERE  pencil_mark_ind > 0
        AND    puzzle_id = p_puzzle_id
        AND    (box_id, answer) IN
               (SELECT a.box_id,
                        a.answer
                 FROM   answers a
                 WHERE  a.puzzle_id = p_puzzle_id
                 AND    a.pencil_mark_ind > 0
                 GROUP  BY a.box_id,
                           a.answer
                 HAVING COUNT(*) = 1);

        p_row_count := SQL%ROWCOUNT;

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('singles: '|| p_row_count);
            output_pencil_marks(p_puzzle_id,v_step_no);
            output_pencil_marks(p_puzzle_id);
            output_answers(p_puzzle_id);
            dbms_output.put_line('--------------------------------------------------------------------------------------------------------------------------------------');
        END IF;

    END;



    PROCEDURE set_singles_rubout
    (
        p_puzzle_id     NUMBER,
        p_show_workings NUMBER,
        p_row_count     OUT NUMBER
    ) AS
        v_step_no NUMBER;
    BEGIN

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('**************************************************************************************************************************************');
            dbms_output.put_line('SINGLES RUBOUT');
            dbms_output.put_line('**************************************************************************************************************************************');
        END IF;

        SELECT step_no_seq.NEXTVAL
        INTO   v_step_no
        FROM   dual;

        p_row_count := 0;


        UPDATE answers a
        SET    pencil_mark_ind = -1,
               step_no         = v_step_no
        WHERE  a.pencil_mark_ind > 0
        AND    a.puzzle_id = p_puzzle_id
        AND    (a.row_id, a.col_id) IN
               (SELECT a2.row_id,
                        a2.col_id
                 FROM   answers a2
                 WHERE  a2.pencil_mark_ind = 0
                 AND    a2.puzzle_id = p_puzzle_id);

        p_row_count := SQL%ROWCOUNT;


        FOR i IN 1 .. 3 LOOP
            UPDATE answers a
            SET    a.pencil_mark_ind = -1,
                   a.step_no         = v_step_no
            WHERE  a.pencil_mark_ind > 0
            AND    a.puzzle_id = p_puzzle_id
            AND    (a.answer, DECODE(i,
                                     1, a.box_id,
                                     2, a.row_id,
                                     3, a.col_id
                                     )
                    ) IN
                    (SELECT  a2.answer,
                            DECODE(i,
                                   1, a2.box_id,
                                   2, a2.row_id,
                                   3, a2.col_id)
                     FROM   answers a2
                     WHERE  a2.puzzle_id = p_puzzle_id
                     AND    a2.pencil_mark_ind = 0
                     AND    (a2.row_id != a.row_id OR a2.col_id != a.col_id)
                    );

            p_row_count := p_row_count + SQL%ROWCOUNT;

        END LOOP;

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('Rubbed out: '|| p_row_count);
            output_pencil_marks(p_puzzle_id, v_step_no);
            output_pencil_marks(p_puzzle_id);
            output_answers(p_puzzle_id);
            dbms_output.put_line('--------------------------------------------------------------------------------------------------------------------------------------');
        END IF;

    END;



    PROCEDURE set_cross_hatches
    (
        p_puzzle_id     NUMBER,
        p_size          NUMBER,  -- n = 1, 2, or 3
        p_show_workings NUMBER,
        p_row_count     OUT NUMBER
    ) AS
        v_step_no NUMBER;
    BEGIN
        IF p_show_workings >= 2 THEN
            dbms_output.put_line('**************************************************************************************************************************************');
            dbms_output.put_line('CROSS HATCHES size: ' || p_size);
            dbms_output.put_line('**************************************************************************************************************************************');
        END IF;

        SELECT step_no_seq.NEXTVAL
        INTO   v_step_no
        FROM   dual;

        reset_pencil_marks(p_puzzle_id);

        p_row_count := 0;


        UPDATE answers
        SET    pencil_mark_ind = 2,
               step_no         = v_step_no
        WHERE  pencil_mark_ind = 1
        AND    puzzle_id = p_puzzle_id
        AND    (box_id, answer) IN
               (SELECT a.box_id,
                        a.answer
                 FROM   answers a
                 WHERE  a.pencil_mark_ind > 0
                 AND    a.puzzle_id = p_puzzle_id
                 GROUP  BY a.box_id,
                           a.answer
                 HAVING COUNT(*) = p_size
                 AND (  MIN(a.col_id) = MAX(a.col_id)
                  OR  MIN(a.row_id) = MAX(a.row_id)
                    )
                );

        p_row_count := SQL%ROWCOUNT;

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('CROSS HATCHES size: ' || p_size ||
                                 ' updated: ' || p_row_count);
            output_pencil_marks(p_puzzle_id, v_step_no);
            dbms_output.put_line('--------------------------------------------------------------------------------------------------------------------------------------');
        END IF;

        IF p_row_count > 0 THEN

            SELECT step_no_seq.NEXTVAL
            INTO   v_step_no
            FROM   dual;


            FOR i IN 1 .. 2 LOOP

                UPDATE answers a
                SET    pencil_mark_ind = -1,
                       step_no         = v_step_no
                WHERE  a.pencil_mark_ind > 0
                AND    a.puzzle_id = p_puzzle_id
                AND    (DECODE(i,
                               1, a.row_id,
                               2, a.col_id
                               ),
                        a.answer
                       ) IN
                       (SELECT DECODE(i,
                                       1, a2.row_id,
                                       2, a2.col_id
                                     ),
                                a2.answer
                         FROM   answers a2
                         WHERE  a2.pencil_mark_ind = 2
                         AND    a2.puzzle_id = p_puzzle_id
                         AND    a2.box_id != a.box_id
                         GROUP  BY a2.box_id,
                                   decode(i,
                                          1, a2.row_id,
                                          2, a2.col_id
                                         ),
                                   a2.answer
                         HAVING COUNT(*) = p_size);

                p_row_count := p_row_count + SQL%ROWCOUNT;

            END LOOP;

        END IF;

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('rubbed out: '|| p_row_count);
            output_pencil_marks(p_puzzle_id,v_step_no);
            output_pencil_marks(p_puzzle_id);
            output_answers(p_puzzle_id);
            dbms_output.put_line('--------------------------------------------------------------------------------------------------------------------------------------');
        END IF;

    END;



    PROCEDURE set_incomplete_sets
    (
        p_puzzle_id     NUMBER,
        p_type          NUMBER, -- 1 = box, 2 = row, 3 = column
        p_size          NUMBER, -- n = 2,3,4, or 5
        p_show_workings NUMBER,
        p_row_count     OUT NUMBER
    ) AS
        v_step_no NUMBER;
    BEGIN
        IF p_show_workings >= 2 THEN
            dbms_output.put_line('**************************************************************************************************************************************');
            dbms_output.put_line('INCOMPLETE SETS size:' || p_size || ' box,row,col: ' || p_type);
            dbms_output.put_line('**************************************************************************************************************************************');
        END IF;

        SELECT step_no_seq.NEXTVAL
        INTO   v_step_no
        FROM   dual;

        reset_pencil_marks(p_puzzle_id);

        p_row_count := 0;


        UPDATE answers
        SET    pencil_mark_ind = 2,
           step_no         = v_step_no
        WHERE  pencil_mark_ind > 0
        AND    puzzle_id = p_puzzle_id
        AND    (row_id, col_id) IN
           (SELECT
                a.row_id,
                    a.col_id
             FROM   (SELECT
                            a.box_id,
                            a.row_id,
                            a.col_id,
                            pn.set_id,
                            COUNT(*) cnt
                     FROM   combinations pn,
                            (SELECT
                                b.box_id,
                                    b.row_id,
                                    b.col_id,
                                    a.cnt,
                                    b.answer
                             FROM   (SELECT
                                    a.box_id,
                                            a.row_id,
                                            a.col_id,
                                            COUNT(*) cnt
                                     FROM   answers a
                                     WHERE  a.puzzle_id = p_puzzle_id
                                     AND    a.pencil_mark_ind > 0
                                     GROUP  BY a.box_id,
                                               a.row_id,
                                               a.col_id
                                     HAVING COUNT(*) <= p_size
                                    ) a,
                                    answers b
                             WHERE  a.box_id = b.box_id
                             AND    a.row_id = b.row_id
                             AND    a.col_id = b.col_id
                             AND    b.puzzle_id = p_puzzle_id
                             AND    b.pencil_mark_ind > 0
                             ) a
                     WHERE  pn.set_size = p_size
                     AND    a.answer = pn.puzzle_number
                     GROUP  BY a.box_id,
                               a.row_id,
                               a.col_id,
                               pn.set_id
                     HAVING COUNT(*) = MAX(a.cnt)
                     ) a,
                    (SELECT
                        DECODE(p_type,
                                   1, a.box_id,
                                   2, a.row_id,
                                   3, a.col_id) id,
                            a.set_id
                     FROM   (SELECT a.box_id,
                                    a.row_id,
                                    a.col_id,
                                    pn.set_id,
                                    COUNT(*) cnt
                             FROM   combinations pn,
                                    (SELECT b.box_id,
                                            b.row_id,
                                            b.col_id,
                                            a.cnt,
                                            b.answer
                                     FROM  (SELECT a.box_id,
                                                   a.row_id,
                                                    a.col_id,
                                                    COUNT(*) cnt
                                             FROM   answers a
                                             WHERE  a.puzzle_id =
                                                    p_puzzle_id
                                             AND    a.pencil_mark_ind > 0
                                             GROUP  BY a.box_id,
                                                       a.row_id,
                                                       a.col_id
                                             HAVING COUNT(*) <= p_size
                                            ) a,
                                            answers b
                                     WHERE  a.box_id = b.box_id
                                     AND    a.row_id = b.row_id
                                     AND    a.col_id = b.col_id
                                     AND    b.puzzle_id = p_puzzle_id
                                     AND    b.pencil_mark_ind > 0) a
                             WHERE  pn.set_size = p_size
                             AND    a.answer = pn.puzzle_number
                             GROUP  BY a.box_id,
                                       a.row_id,
                                       a.col_id,
                                       pn.set_id
                             HAVING COUNT(*) = MAX(a.cnt)
                             ) a
                     GROUP  BY DECODE(p_type,
                                      1, a.box_id,
                                      2, a.row_id,
                                      3, a.col_id),
                               a.set_id
                     HAVING COUNT(*) = p_size
                     ) s
             WHERE  s.id = DECODE(p_type,
                                  1, a.box_id,
                                  2, a.row_id,
                                  3, a.col_id)
             AND    s.set_id = a.set_id
             );

        p_row_count := SQL%ROWCOUNT;

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('CROSS HATCHES size: ' || p_size ||
                                 ' updated: ' || p_row_count);
            output_pencil_marks(p_puzzle_id,v_step_no);
            dbms_output.put_line('--------------------------------------------------------------------------------------------------------------------------------------');
        END IF;


        IF p_row_count > 0 THEN

            SELECT step_no_seq.NEXTVAL
            INTO   v_step_no
            FROM   dual;

            UPDATE answers a
            SET    pencil_mark_ind = -1,
                   step_no         = v_step_no
            WHERE  a.pencil_mark_ind > 0
            AND    a.pencil_mark_ind != 2
            AND    a.puzzle_id = p_puzzle_id
            AND    (DECODE(p_type,
                           1, a.box_id,
                           2, a.row_id,
                           3, a.col_id
                          ),
                    a.answer
                   ) IN
                   (SELECT  DECODE(p_type,
                                   1, a2.box_id,
                                   2, a2.row_id,
                                   3, a2.col_id),
                            a2.answer
                     FROM   answers a2
                     WHERE  a2.pencil_mark_ind = 2
                     AND    a2.puzzle_id = p_puzzle_id);

          p_row_count := SQL%ROWCOUNT;

        END IF;

        IF p_show_workings >= 2 THEN
            dbms_output.put_line('rubbed out: '|| p_row_count);
            output_pencil_marks(p_puzzle_id,v_step_no);
            output_pencil_marks(p_puzzle_id);
            output_answers(p_puzzle_id);
            dbms_output.put_line('--------------------------------------------------------------------------------------------------------------------------------------');
        END IF;

    END;


    PROCEDURE set_singles
    (
        p_puzzle_id     NUMBER,
        p_show_workings NUMBER,
        p_row_count     OUT NUMBER
    ) AS
        v_answer_count      NUMBER;
        v_answer_count_last NUMBER;
        v_row_count         NUMBER;
        v_limit_cnt         NUMBER := 0;
    BEGIN
        p_row_count := 0;
        v_answer_count_last := answer_count(p_puzzle_id);
        LOOP
            v_limit_cnt := v_limit_cnt + 1;

            set_singles_cell
            (
              p_puzzle_id,
                p_show_workings,
                v_row_count
            );

            p_row_count := p_row_count + v_row_count;
            IF v_row_count > 0 THEN
                set_singles_rubout
                (
                  p_puzzle_id,
                    p_show_workings,
                    v_row_count
                );
            END IF;

            set_singles_box
            (
              p_puzzle_id,
                p_show_workings,
                v_row_count
            );

            p_row_count := p_row_count + v_row_count;
            IF v_row_count > 0 THEN

                set_singles_rubout
                (
                  p_puzzle_id,
                    p_show_workings,
                    v_row_count
                );

            END IF;

            v_answer_count := answer_count(p_puzzle_id);

            EXIT WHEN v_answer_count = v_answer_count_last OR v_answer_count = 81 OR v_limit_cnt > 60;

            v_answer_count_last := v_answer_count;

        END LOOP;
    END;

    PROCEDURE reset_puzzle
    (
      p_puzzle_id     NUMBER
    )
    AS
    BEGIN

        DELETE FROM answers
        WHERE  puzzle_id = p_puzzle_id;


        INSERT INTO answers
            (puzzle_id,
             step_no,
             row_id,
             col_id,
             pencil_mark_ind,
             answer,
             box_id)
            SELECT puzzle_id,
                   -1,
                   row_id,
                   1,
                   0,
                   col_id1,
                   NULL
            FROM   puzzle_load
            WHERE  puzzle_id = p_puzzle_id
            AND    col_id1 IS NOT NULL
            UNION ALL
            SELECT puzzle_id,
                   -1,
                   row_id,
                   2,
                   0,
                   col_id2,
                   NULL
            FROM   puzzle_load
            WHERE  puzzle_id = p_puzzle_id
            AND    col_id2 IS NOT NULL
            UNION ALL
            SELECT puzzle_id,
                   -1,
                   row_id,
                   3,
                   0,
                   col_id3,
                   NULL
            FROM   puzzle_load
            WHERE  puzzle_id = p_puzzle_id
            AND    col_id3 IS NOT NULL
            UNION ALL
            SELECT puzzle_id,
                   -1,
                   row_id,
                   4,
                   0,
                   col_id4,
                   NULL
            FROM   puzzle_load
            WHERE  puzzle_id = p_puzzle_id
            AND    col_id4 IS NOT NULL
            UNION ALL
            SELECT puzzle_id,
                   -1,
                   row_id,
                   5,
                   0,
                   col_id5,
                   NULL
            FROM   puzzle_load
            WHERE  puzzle_id = p_puzzle_id
            AND    col_id5 IS NOT NULL
            UNION ALL
            SELECT puzzle_id,
                   -1,
                   row_id,
                   6,
                   0,
                   col_id6,
                   NULL
            FROM   puzzle_load
            WHERE  puzzle_id = p_puzzle_id
            AND    col_id6 IS NOT NULL
            UNION ALL
            SELECT puzzle_id,
                   -1,
                   row_id,
                   7,
                   0,
                   col_id7,
                   NULL
            FROM   puzzle_load
            WHERE  puzzle_id = p_puzzle_id
            AND    col_id7 IS NOT NULL
            UNION ALL
            SELECT puzzle_id,
                   -1,
                   row_id,
                   8,
                   0,
                   col_id8,
                   NULL
            FROM   puzzle_load
            WHERE  puzzle_id = p_puzzle_id
            AND    col_id8 IS NOT NULL
            UNION ALL
            SELECT puzzle_id,
                   -1,
                   row_id,
                   9,
                   0,
                   col_id9,
                   NULL
            FROM   puzzle_load
            WHERE  puzzle_id = p_puzzle_id
            AND    col_id9 IS NOT NULL;

        COMMIT;

        EXECUTE IMMEDIATE 'ANALYZE TABLE answers COMPUTE STATISTICS';
    END;

BEGIN

    DBMS_OUTPUT.ENABLE (buffer_size=>null);
    reset_puzzle(p_puzzle_id);


    output_answers(p_puzzle_id);


    setup_candidates(p_puzzle_id);

    IF p_show_workings >= 1 THEN

        output_pencil_marks(p_puzzle_id);
    END IF;



    v_answer_count_last := answer_count(p_puzzle_id);
    dbms_output.put_line('SINGLES  answers: ' || v_answer_count_last);

    set_singles
    (
      p_puzzle_id,
      p_show_workings,
      v_row_count
    );

    v_answer_count_last := answer_count(p_puzzle_id);




    v_limit_cnt := 0;
    WHILE v_answer_count_last < 81 LOOP

        v_limit_cnt := v_limit_cnt + 1;
        IF p_show_workings >= 1 THEN
            dbms_output.put_line('CROSS HATCH #' || v_limit_cnt ||
                                 '  answers: ' || v_answer_count);
        END IF;

        FOR i IN 2 .. 3 LOOP
            set_cross_hatches
            (
              p_puzzle_id,
                i,
                p_show_workings,
                v_row_count
            );

            IF v_row_count > 0 THEN
                set_singles
                (
                  p_puzzle_id,
                    p_show_workings,
                    v_singles_count
                );
            END IF;

            IF p_show_workings >= 1 THEN

                v_answer_count := answer_count(p_puzzle_id);
                IF p_show_workings >= 1 THEN
                    dbms_output.put_line('     Size: ' || i ||
                                         ' answers: ' ||
                                         v_answer_count ||
                                         ' updated: ' || v_row_count ||
                                         ' singles: ' ||
                                         v_singles_count);
                END IF;
                EXIT WHEN v_answer_count = 81;

            END IF;
        END LOOP;

        v_answer_count := answer_count(p_puzzle_id);

        EXIT WHEN v_answer_count = v_answer_count_last OR v_answer_count = 81 OR v_limit_cnt > 60;

        v_answer_count_last := v_answer_count;

    END LOOP;


    v_answer_count_last := answer_count(p_puzzle_id);
    v_limit_cnt := 0;
    WHILE v_answer_count_last < 81 LOOP
        v_limit_cnt := v_limit_cnt + 1;
        IF p_show_workings >= 1 THEN
            dbms_output.put_line('PARTIAL MEMBER SET #' || v_limit_cnt ||
                                 ' answers: ' || v_answer_count_last);
        END IF;

        FOR i IN 2 .. 5 LOOP
            FOR j IN 1 .. 3 LOOP
                set_incomplete_sets(p_puzzle_id,
                                  j,
                                  i,
                                  p_show_workings,
                                  v_row_count);
                IF v_row_count > 0 THEN
                    set_singles(p_puzzle_id,
                                p_show_workings,
                                v_singles_count);
                END IF;

                v_answer_count := answer_count(p_puzzle_id);
                IF p_show_workings >= 1 THEN
                    dbms_output.put_line('     Size: ' || i ||
                                         ' Box,Row,Col: ' || j ||
                                         ' answers: ' ||
                                         v_answer_count ||
                                         ' updated: ' || v_row_count ||
                                         ' singles: ' ||
                                         v_singles_count);
                END IF;
                EXIT WHEN v_answer_count = 81;

            END LOOP;
        END LOOP;

        v_answer_count := answer_count(p_puzzle_id);

        EXIT WHEN v_answer_count = v_answer_count_last OR v_answer_count = 81 OR v_limit_cnt > 60;

        v_answer_count_last := v_answer_count;

    END LOOP;

    IF p_show_workings >= 2 THEN
        output_pencil_marks(p_puzzle_id);
        output_answers(p_puzzle_id);
    END IF;

    COMMIT;

    v_answer_count := answer_count(p_puzzle_id);

    IF p_show_workings >= 2 THEN
        dbms_output.put_line('Answer Count: ' || v_answer_count);
    END IF;

    COMMIT;

    dbms_output.put_line(' ');
    dbms_output.put_line(' ');
    IF v_answer_count = 81 THEN
        dbms_output.put_line('The puzzle has been successfully solved');
    ELSE
        dbms_output.put_line('Failed to solve puzzle completely');
        output_pencil_marks(p_puzzle_id);
    END IF;

    output_answers(p_puzzle_id);

    dbms_output.put_line(' ');
    dbms_output.put_line(' ');

    dbms_output.put_line('PL/SQL Sudoku Solver - (c) 2005 Philip Lambert, Database Innovation Limited');

END;
//

CREATE OR REPLACE PROCEDURE setup_candidates(p_puzzle_id NUMBER) AS
        v_box_id NUMBER;
        v_row_id NUMBER;
        v_col_id NUMBER;

        TYPE t_answers IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
        v_pencil_marks t_answers;

        CURSOR c_cells IS
            SELECT box_id,
                   row_id,
                   col_id
            FROM   cells
            MINUS
            SELECT box_id,
                   row_id,
                   col_id
            FROM   answers
            WHERE  pencil_mark_ind = 0
            AND    puzzle_id = p_puzzle_id
            ORDER  BY row_id,
                      col_id;

        CURSOR c_pencil_marks IS
            SELECT a.answer
            FROM   (SELECT puzzle_number answer
                    FROM   puzzle_numbers
                    MINUS
                    -- Answers in that row
                    SELECT answer
                    FROM   answers
                    WHERE  pencil_mark_ind = 0
                    AND    row_id = v_row_id
                    AND    puzzle_id = p_puzzle_id
                    MINUS
                    -- Answers in that column
                    SELECT answer
                    FROM   answers
                    WHERE  pencil_mark_ind = 0
                    AND    col_id = v_col_id
                    AND    puzzle_id = p_puzzle_id
                    MINUS
                    -- Answers in that box
                    SELECT answer
                    FROM   answers
                    WHERE  pencil_mark_ind = 0
                    AND    box_id = v_box_id
                    AND    puzzle_id = p_puzzle_id) a;
    BEGIN
        -- Set denormalised box_id on answer
        UPDATE answers a
        SET    box_id = (SELECT box_id
                         FROM   cells c
                         WHERE  c.row_id = a.row_id
                         AND    c.col_id = a.col_id);

        -- Determine the initial pencil marks
        FOR r_cells IN c_cells LOOP

            v_box_id := r_cells.box_id;
            v_row_id := r_cells.row_id;
            v_col_id := r_cells.col_id;

            OPEN c_pencil_marks;
            FETCH c_pencil_marks BULK COLLECT
                INTO v_pencil_marks;
            CLOSE c_pencil_marks;

            FORALL i IN 1 .. v_pencil_marks.COUNT
                INSERT INTO answers
                    (puzzle_id,
                     step_no,
                     box_id,
                     row_id,
                     col_id,
                     pencil_mark_ind,
                     answer)
                VALUES
                    (p_puzzle_id,
                     10,
                     v_box_id,
                     v_row_id,
                     v_col_id,
                     1,
                     v_pencil_marks(i));

            COMMIT;
        END LOOP;

        COMMIT;
    END;
//

CREATE OR REPLACE  PROCEDURE output_answers(p_puzzle_id NUMBER) AS
        v_row_id NUMBER;

        CURSOR c_answers IS
            SELECT c.row_id,
                   c.col_id,
                   a.answer
            FROM   cells   c,
                   answers a
            WHERE  a.puzzle_id(+) = p_puzzle_id
            AND    a.row_id(+) = c.row_id
            AND    a.col_id(+) = c.col_id
            AND    a.pencil_mark_ind(+) = 0
            ORDER  BY c.row_id,
                      c.col_id;
    BEGIN
        v_row_id := 0;
        FOR r_answers IN c_answers LOOP

            IF v_row_id != r_answers.row_id THEN
                dbms_output.put_line(' ');
                IF r_answers.row_id IN (1, 4, 7) THEN
                    dbms_output.put_line('|===========|===========|===========|');
                ELSE
                    dbms_output.put_line('|-----------|-----------|-----------|');
                END IF;
                v_row_id := r_answers.row_id;
                dbms_output.put('|');
            END IF;
            dbms_output.put(' ' || nvl(TRIM(to_char(r_answers.answer)),
                                       ' '));
            dbms_output.put(' |');
        END LOOP;
        dbms_output.put_line(' ');
        dbms_output.put_line('|===========|===========|===========|');
    END;
//

delimiter ;//