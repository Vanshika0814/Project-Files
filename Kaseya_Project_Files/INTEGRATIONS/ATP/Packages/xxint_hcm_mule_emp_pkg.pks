create or replace PACKAGE             XXINT.XXINT_HCM_MULE_EMP_PKG IS

  -- Define object type inside package spec
    TYPE emp_data_rec IS RECORD (
            person_number   VARCHAR2(100),
            person_id       NUMBER,
            person_image_id NUMBER,
            invoked_by      VARCHAR2(200)
    );

  -- Define collection type inside package spec
    TYPE emp_data_tab IS
        TABLE OF emp_data_rec;

  -- Procedure to upsert employee data
    PROCEDURE merge_emp_data (
        p_data    IN emp_data_tab,
        p_status  OUT VARCHAR2,
        p_message OUT VARCHAR2
    );

    PROCEDURE split_person_data_by_date (
        p_from_date      IN  DATE,
        p_to_date        IN  DATE,
        p_group_size     IN  NUMBER,
        p_result_cursor  OUT SYS_REFCURSOR
    );

    PROCEDURE split_person_number (
      p_person_number IN VARCHAR2,
      p_group_size IN NUMBER,
      p_result_cursor OUT SYS_REFCURSOR
  );

END XXINT_HCM_MULE_EMP_PKG;