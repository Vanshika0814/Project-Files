create or replace TYPE       XXINT.EMP_DATA_OBJ AS OBJECT (
    person_number   VARCHAR2(100),
    person_id       NUMBER,
    person_image_id NUMBER,
    invoked_by      VARCHAR2(200)
);