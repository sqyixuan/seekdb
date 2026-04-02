delimiter //;
CREATE OR REPLACE PACKAGE "CAS_SYS"
IS

PROCEDURE reg_sys_log(pi_fcn_nm         VARCHAR2
                       ,pi_pol_num        VARCHAR2
                       ,pi_log_typ        VARCHAR2
                       ,pi_fcn_return     NUMBER
                       ,pi_chk_pt         NUMBER    DEFAULT NULL
                       ,pi_sqlcode        NUMBER    DEFAULT NULL
                       ,pi_sqlerrm        VARCHAR2  DEFAULT NULL
                       ,pi_app_cd         VARCHAR2  DEFAULT 'CAS'
                       ,pi_agt_code       VARCHAR2  DEFAULT NULL
                       ,pi_app_num        VARCHAR2  DEFAULT NULL
                       ,pi_proposal_id    VARCHAR2  DEFAULT NULL
                       ,pi_pol_num_02     VARCHAR2  DEFAULT NULL
                       );

  PROCEDURE reg_sys_log(po_sys_log_seq    OUT        NUMBER
                       ,pi_fcn_nm         IN         VARCHAR2
                       ,pi_pol_num        IN         VARCHAR2
                       ,pi_log_typ        IN         VARCHAR2
                       ,pi_fcn_return     IN         NUMBER
                       ,pi_chk_pt         IN         NUMBER    DEFAULT NULL
                       ,pi_sqlcode        IN         NUMBER    DEFAULT NULL
                       ,pi_sqlerrm        IN         VARCHAR2  DEFAULT NULL
                       ,pi_app_cd         IN         VARCHAR2  DEFAULT 'CAS'
                       ,pi_agt_code       IN         VARCHAR2  DEFAULT NULL
                       ,pi_app_num        IN         VARCHAR2  DEFAULT NULL
                       ,pi_proposal_id    IN         VARCHAR2  DEFAULT NULL
                       ,pi_pol_num_02     IN         VARCHAR2  DEFAULT NULL
                       );

  

  FUNCTION cas_dt
    RETURN DATE;

  
END cas_sys;


//
CREATE OR REPLACE PACKAGE "CAS_USER"
IS
  TYPE gp_type_str_tab IS TABLE OF varchar2(10) INDEX BY PLS_INTEGER;
  FUNCTION get_user_profile_rec(pi_user_id          IN         VARCHAR2
                               ) RETURN tuser_profiles%ROWTYPE;
  FUNCTION get_user_profile_rec RETURN tuser_profiles%ROWTYPE;
  FUNCTION get_avail_terr_tab(pi_user_id          IN        VARCHAR2
                             ,pi_app_cd           IN        VARCHAR2
                             ) RETURN gp_type_str_tab;
  FUNCTION get_avail_terr_tab(pi_app_cd          IN        VARCHAR2
                              ) RETURN gp_type_str_tab;
  FUNCTION get_avail_terr_tab RETURN gp_type_str_tab;
  FUNCTION is_legal_terr ( pi_terr_cd          IN            VARCHAR2
                          ,pi_user_id          IN            VARCHAR2
                          ,pi_app_cd           IN            VARCHAR2
                         ) RETURN VARCHAR2;
  FUNCTION is_legal_terr ( pi_terr_cd          IN            VARCHAR2
                          ,pi_app_cd           IN            VARCHAR2
                         ) RETURN VARCHAR2;
  FUNCTION is_legal_terr ( pi_terr_cd          IN            VARCHAR2
                         ) RETURN VARCHAR2;
  FUNCTION get_ebs_agt_cd(pi_terr_cd          IN         VARCHAR2
                         ,pi_user_id          IN         VARCHAR2
                         ) RETURN VARCHAR2;
  FUNCTION get_ebs_agt_cd(pi_terr_cd          IN         VARCHAR2
                         ) RETURN VARCHAR2;
  FUNCTION terr_cd RETURN VARCHAR2;
  FUNCTION get_user_terr_cd(pi_user_id          IN         VARCHAR2
                           ) RETURN varchar2;
  
  FUNCTION get_terr_cd_list_str(pi_app_cd         IN         VARCHAR2
                               ,pi_all_terr_ind   in         varchar2
                               ) RETURN VARCHAR2;

  FUNCTION get_terr_cd_list_str(pi_app_cd         IN         VARCHAR2
                               ) RETURN VARCHAR2;
  FUNCTION get_terr_cd_list_str RETURN VARCHAR2;
 
  FUNCTION is_menu_grant(pi_menu_cd  IN VARCHAR2
                        ,pi_app_cd   IN VARCHAR2)
            RETURN VARCHAR2;

  FUNCTION is_conn_ext_user(pi_user IN VARCHAR2) RETURN VARCHAR2;

END;
//
CREATE OR REPLACE PACKAGE "CTL_PARM"
IS
  PROCEDURE init_parm;     -- gz
  PROCEDURE get(pi_parm_typ  IN tcontrol_parameters.parm_typ%TYPE
               ,po_rtrn_valu OUT VARCHAR2
               );
  PROCEDURE get(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
               ,po_rtrn_valu OUT NUMBER
               );
  PROCEDURE get(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
               ,po_rtrn_valu OUT DATE
               );
  PROCEDURE get_real(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
                    ,po_rtrn_valu OUT DATE
                    );
  PROCEDURE chng(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
                ,pi_set_valu  IN  VARCHAR2
                );
  PROCEDURE chng(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
                ,pi_set_valu  IN  NUMBER
                );
  PROCEDURE chng(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
                ,pi_set_valu  IN  DATE
                );
  PROCEDURE get(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
               ,pi_crcy_code IN  tcurrency_master.crcy_code%type
               ,po_rtrn_valu OUT VARCHAR2
               );
  PROCEDURE get(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
               ,pi_crcy_code IN  tcurrency_master.crcy_code%type
               ,po_rtrn_valu OUT NUMBER
               );
  PROCEDURE get(pi_parm_typ  IN  VARCHAR2
               ,pi_crcy_code IN  VARCHAR2
               ,pi_plan_code IN  VARCHAR2
               ,pi_eff_dt    IN  DATE
               ,po_rtrn_valu OUT VARCHAR2
               ,pi_vers_num  IN  VARCHAR2  DEFAULT NULL
               );
  PROCEDURE get(pi_parm_typ  IN  VARCHAR2
               ,pi_crcy_code IN  VARCHAR2
               ,pi_plan_code IN  VARCHAR2
               ,pi_eff_dt    IN  DATE
               ,po_rtrn_valu OUT NUMBER
               ,pi_vers_num  IN  VARCHAR2  DEFAULT NULL
               );

  FUNCTION get_char(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                   ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                   ,pi_crcy_code    IN  tcontrol_parameters.crcy_code%TYPE
                   ) RETURN VARCHAR2;
  FUNCTION get_char(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                   ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                   ) RETURN VARCHAR2;
  FUNCTION get_char(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                   ) RETURN VARCHAR2;
  FUNCTION get_dt(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                 ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                 ,pi_crcy_code    IN  tcontrol_parameters.crcy_code%TYPE
                 ) RETURN DATE;
  FUNCTION get_dt(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                 ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                 ) RETURN DATE;
  FUNCTION get_dt(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                 ) RETURN DATE;
  FUNCTION get_num(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                  ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                  ,pi_crcy_code    IN  tcontrol_parameters.crcy_code%TYPE
                  ) RETURN NUMBER;
  FUNCTION get_num(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                  ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                  ) RETURN NUMBER;
  FUNCTION get_num(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                  ) RETURN NUMBER;
END ctl_parm;
//
CREATE OR REPLACE PACKAGE "DO"
IS
 TYPE TBL_OUTPUT IS TABLE OF VARCHAR2 (255)
 INDEX BY BINARY_INTEGER;
 
 PROCEDURE switch (show_in IN VARCHAR2 := NULL);

 PROCEDURE pl
  (date_in IN DATE,
   mask_in IN VARCHAR2 := 'Month DD, YYYY - HH:MI:SS PM',
   show_in IN VARCHAR2 := NULL);

 PROCEDURE pl (number_in IN NUMBER, show_in IN VARCHAR2 := NULL);

 PROCEDURE pl (hint_in IN VARCHAR2
              ,char_in IN VARCHAR2 := NULL
              ,show_in IN VARCHAR2 := NULL);

 PROCEDURE pl
  (char_in IN VARCHAR2,
   number_in IN NUMBER,
   show_in IN VARCHAR2 := NULL);
 PROCEDURE pl
  (char_in IN VARCHAR2,
   date_in IN DATE,
   mask_in IN VARCHAR2 := 'Month DD, YYYY - HH:MI:SS PM',
   show_in IN VARCHAR2 := NULL);

 PROCEDURE pl (boolean_in IN BOOLEAN, show_in IN VARCHAR2 := NULL);

 PROCEDURE pl
  (char_in IN VARCHAR2,
   boolean_in IN BOOLEAN,
   show_in IN VARCHAR2 := NULL);
        FUNCTION WRAP(text_in      IN VARCHAR2
                     ,line_length  IN INTEGER  := 255
                     ,delimited_by IN VARCHAR2 := ' '
                     )
        RETURN TBL_OUTPUT;
END do;
//
CREATE OR REPLACE PACKAGE "FCN"
IS
 Procedure Set_Redo_Dt(pi_redo_dt in date);
  Function  Redo_Dt          return date;
END;
//

CREATE OR REPLACE PACKAGE "TERR_FCN" is
    procedure set_terr_cd(pi_terr_cd in varchar2);
    function  get_terr_cd return varchar2;

  function  get_terr_cd_withoutHO return varchar2;   

    procedure set_avail_terrs( pi_user_id in varchar2);
    function get_avail_terrs return varchar2;

    function get_avail_terrs_withoutHO return varchar2;

    procedure set_avail_terrs( pi_user_id in varchar2
                              ,pi_all_terr_ind in varchar2);
    function get_avail_terrs (pi_all_terr_ind in varchar2) return varchar2;

    procedure terr_switch(pi_terr_cd in varchar2);
    procedure set_app_cd(pi_app_cd in varchar2);
    function get_app_cd return varchar2;

    function get_all_terrs return varchar2;

end;
//

CREATE OR REPLACE FUNCTION "GET_CAS_SYS_DT"
  RETURN  date
IS
  l_cas_dt  DATE:=to_date('2004-05-07','yyyy-mm-dd');
BEGIN
    --ctl_parm.get('CAS_DT', l_cas_dt);
    RETURN (l_cas_dt) ;
END;
//
CREATE OR REPLACE FUNCTION "GET_SYS_DT"
  RETURN  date
IS
  l_dt  DATE;
BEGIN
    ctl_parm.get('DT', l_dt);
    RETURN (l_dt) ;
END;
//
CREATE OR REPLACE FUNCTION "GET_REAL_DT"
  RETURN  date
IS
  l_dt  DATE;
BEGIN
    ctl_parm.get_real('DT', l_dt);
    RETURN (l_dt) ;
END;
//
CREATE OR REPLACE FUNCTION "GET_TERR_CD" (pi_pol_num IN tpolicys.pol_num%type)
    RETURN VARCHAR2
IS
    CURSOR c_terr_cd IS
        SELECT terr_cd
        FROM   tpolicys
        WHERE  pol_num = pi_pol_num
        UNION
        SELECT terr_cd
        FROM   twip_policys
        WHERE  pol_num = pi_pol_num;
    CURSOR c_terr_cd_all IS
        SELECT terr_cd
        FROM   vpolicys_all
        WHERE  pol_num = pi_pol_num;

    CURSOR c_terr_cd_eapp IS
        SELECT b.terr_cd
        FROM   tap_applications a
        INNER JOIN  b ON (a.app_num = b.app_num)
        WHERE  a.pol_num = pi_pol_num;

    l_terr_cd   tpolicys.pol_num%type;
BEGIN
    OPEN c_terr_cd;
    FETCH c_terr_cd INTO l_terr_cd;
    IF c_terr_cd%NOTFOUND THEN
        l_terr_cd := NULL;
    END IF;
    CLOSE c_terr_cd;

    IF l_terr_cd IS NULL THEN
      OPEN c_terr_cd_all;
      FETCH c_terr_cd_all INTO l_terr_cd;
      CLOSE c_terr_cd_all;
    END IF;

    IF l_terr_cd IS NULL THEN
      OPEN c_terr_cd_eapp;
      FETCH c_terr_cd_eapp INTO l_terr_cd;
      CLOSE c_terr_cd_eapp;
    END IF;

    RETURN (l_terr_cd);
END;
//

CREATE OR REPLACE FUNCTION "GET_REAL_CAS_DT"
  RETURN  date
IS
  l_cas_dt  DATE;
BEGIN
    ctl_parm.get_real('CAS_DT', l_cas_dt);
    RETURN (l_cas_dt) ;
END;
//

CREATE OR REPLACE FUNCTION "SF_GET_TERR_CD" (PI_USER_ID IN TUSER_PROFILES.USER_ID%TYPE)
RETURN VARCHAR2
IS
   L_TERR_CD TUSER_PROFILES.TERR_CD%TYPE;
   CURSOR C_TERR_CD IS
    SELECT TERR_CD
    FROM TUSER_PROFILES
    WHERE USER_ID = PI_USER_ID;
BEGIN
  IF pi_user_id = USER THEN
    l_terr_cd := cas_user.terr_cd;
  ELSE
   OPEN C_TERR_CD;
   FETCH C_TERR_CD INTO L_TERR_CD;
   CLOSE C_TERR_CD;
  END IF;
   RETURN (L_TERR_CD);
END;
//
CREATE OR REPLACE PACKAGE BODY "CAS_SYS"
IS
g_dt_fmt                             tcontrol_parameters.parm_valu%TYPE;
  PROCEDURE reg_sys_log(po_sys_log_seq    OUT        NUMBER
                       ,pi_fcn_nm         IN         VARCHAR2
                       ,pi_pol_num        IN         VARCHAR2
                       ,pi_log_typ        IN         VARCHAR2
                       ,pi_fcn_return     IN         NUMBER
                       ,pi_chk_pt         IN         NUMBER    DEFAULT NULL
                       ,pi_sqlcode        IN         NUMBER    DEFAULT NULL
                       ,pi_sqlerrm        IN         VARCHAR2  DEFAULT NULL
                       ,pi_app_cd         IN         VARCHAR2  DEFAULT 'CAS'
                       ,pi_agt_code       IN         VARCHAR2  DEFAULT NULL
                       ,pi_app_num        IN         VARCHAR2  DEFAULT NULL
                       ,pi_proposal_id    IN         VARCHAR2  DEFAULT NULL
                       ,pi_pol_num_02     IN         VARCHAR2  DEFAULT NULL
                       )
  IS
    l_sys_log_rec                 tsystem_logs%ROWTYPE;
    l_seq_num                     NUMBER;

    PRAGMA                        AUTONOMOUS_TRANSACTION;
  BEGIN
    SELECT seq_sys_log.NEXTVAL INTO l_seq_num FROM DUAL;
    l_sys_log_rec.seq_num := TO_CHAR(cas_dt,'YYYYMMDD')||LPAD(l_seq_num,7,'0');
    po_sys_log_seq := l_sys_log_rec.seq_num;
    l_sys_log_rec.trxn_cas_dt := SYSDATE;
    l_sys_log_rec.trxn_sys_dt := SYSDATE;
    l_sys_log_rec.user_id := USER;
    l_sys_log_rec.fcn_nm := SUBSTR(pi_fcn_nm,1,61);
    l_sys_log_rec.pol_num := pi_pol_num;
    l_sys_log_rec.log_typ := pi_log_typ;
    l_sys_log_rec.chk_pt := pi_chk_pt;
    l_sys_log_rec.fcn_return := pi_fcn_return;
    l_sys_log_rec.SQLCODE := pi_sqlcode;
    l_sys_log_rec.SQLERRM := SUBSTR(pi_sqlerrm,1,200);
    l_sys_log_rec.app_cd := pi_app_cd;

    l_sys_log_rec.SQLERRM        := SUBSTR(pi_sqlerrm,1,500)         ;       --  change to 500
    l_sys_log_rec.agt_code       := SUBSTR(pi_agt_code,1,8)          ;
    l_sys_log_rec.app_num        := SUBSTR(pi_app_num,1,30)          ;
    l_sys_log_rec.proposal_id    := SUBSTR(pi_proposal_id,1,100)     ;
    l_sys_log_rec.pol_num_02     := SUBSTR(pi_pol_num_02,1,30)       ;
    INSERT INTO tsystem_logs VALUES l_sys_log_rec;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END reg_sys_log;

  PROCEDURE reg_sys_log(pi_fcn_nm         VARCHAR2
                       ,pi_pol_num        VARCHAR2
                       ,pi_log_typ        VARCHAR2
                       ,pi_fcn_return     NUMBER
                       ,pi_chk_pt         NUMBER    DEFAULT NULL
                       ,pi_sqlcode        NUMBER    DEFAULT NULL
                       ,pi_sqlerrm        VARCHAR2  DEFAULT NULL
                       ,pi_app_cd         VARCHAR2  DEFAULT 'CAS'
                       ,pi_agt_code       VARCHAR2  DEFAULT NULL
                       ,pi_app_num        VARCHAR2  DEFAULT NULL
                       ,pi_proposal_id    VARCHAR2  DEFAULT NULL
                       ,pi_pol_num_02     VARCHAR2  DEFAULT NULL
                       )
  IS
    l_dummy_sys_log_seq           NUMBER;
  BEGIN
    reg_sys_log(po_sys_log_seq         => l_dummy_sys_log_seq
               ,pi_fcn_nm              => pi_fcn_nm
               ,pi_pol_num             => pi_pol_num
               ,pi_log_typ             => pi_log_typ
               ,pi_fcn_return          => pi_fcn_return
               ,pi_chk_pt              => pi_chk_pt
               ,pi_sqlcode             => pi_sqlcode
               ,pi_sqlerrm             => pi_sqlerrm
               ,pi_app_cd              => pi_app_cd
               ,pi_agt_code            => pi_agt_code
               ,pi_app_num             => pi_app_num
               ,pi_proposal_id         => pi_proposal_id
               ,pi_pol_num_02          => pi_pol_num_02
               );
  END reg_sys_log;

  FUNCTION cas_dt
    RETURN DATE
  IS
    l_dt_in_str              tcontrol_parameters.parm_valu%TYPE;
    CURSOR c_ctl_parm IS
      SELECT parm_valu
        FROM tcontrol_parameters
       WHERE parm_typ = 'CAS_DT'
       ;
  BEGIN
    OPEN c_ctl_parm;
    FETCH c_ctl_parm INTO l_dt_in_str;
    CLOSE c_ctl_parm;

    RETURN TO_DATE(l_dt_in_str,g_dt_fmt);
  END cas_dt;

END cas_sys;
//
CREATE OR REPLACE PACKAGE BODY "CAS_USER"
IS

  g_user_profile_rec              tuser_profiles%ROWTYPE;

  TYPE type_user_info_rec          IS RECORD (terr_cd      tuser_profiles.terr_cd%TYPE
                                              );
  g_user_info_rec                 type_user_info_rec;

  TYPE type_avail_terr_str_tab IS TABLE OF VARCHAR2(4000) INDEX BY VARCHAR2(20);
  g_avail_terr_str_tab            type_avail_terr_str_tab; 
  g_avail_terr_tab_cas            gp_type_str_tab;
  g_avail_terr_tab_cics           gp_type_str_tab;
  g_avail_terr_tab_glh            gp_type_str_tab;
  g_avail_terr_tab_gp             gp_type_str_tab;
  g_avail_terr_tab_ams            gp_type_str_tab;
  g_avail_terr_tab_cics_gp        gp_type_str_tab;
  g_avail_terr_tab_CSC            gp_type_str_tab;
  g_avail_terr_tab_MIS            gp_type_str_tab;
  g_avail_terr_tab_SVR            gp_type_str_tab;


  PROCEDURE init_pkg IS
    l_avail_terr_str              VARCHAR2(4000);
  BEGIN
    g_user_profile_rec := get_user_profile_rec(USER);

    g_user_info_rec.terr_cd := g_user_profile_rec.terr_cd;

    g_avail_terr_str_tab.DELETE;
    FOR rec IN (SELECT DISTINCT app_cd FROM tuser_terr_privs
               ) LOOP
      l_avail_terr_str := 'ABC';
      g_avail_terr_str_tab(rec.app_cd) := l_avail_terr_str;
    
    END LOOP;

  END init_pkg;



  FUNCTION get_user_profile_rec(pi_user_id          IN         VARCHAR2
                               ) RETURN tuser_profiles%ROWTYPE
  IS
    CURSOR c_user IS
      SELECT * FROM tuser_profiles
       WHERE user_id = pi_user_id
       ;
    l_user_rec                    tuser_profiles%ROWTYPE;
  BEGIN
    OPEN c_user;
    FETCH c_user INTO l_user_rec;
    CLOSE c_user;

    RETURN l_user_rec;

  END get_user_profile_rec;

  FUNCTION get_user_profile_rec RETURN tuser_profiles%ROWTYPE
  IS
  BEGIN
    RETURN g_user_profile_rec;
  END get_user_profile_rec;


  FUNCTION get_avail_terr_tab(pi_user_id          IN        VARCHAR2
                             ,pi_app_cd           IN        VARCHAR2
                             ) RETURN gp_type_str_tab
  IS
    l_idx                         PLS_INTEGER := 0;
    l_avail_terr_str              VARCHAR2(4000);
    l_list_tab                    gp_type_str_tab;
  BEGIN
    IF pi_user_id = USER
      AND g_avail_terr_str_tab.EXISTS(pi_app_cd)
    THEN
      IF pi_app_cd = 'CAS' THEN
        RETURN g_avail_terr_tab_cas;
      ELSIF pi_app_cd = 'CICS' THEN
        RETURN g_avail_terr_tab_cics;
      ELSIF pi_app_cd = 'GLH' THEN
        RETURN g_avail_terr_tab_glh;
      ELSIF pi_app_cd = 'GP' THEN
        RETURN g_avail_terr_tab_gp;
      ELSIF pi_app_cd = 'AMS' THEN
        RETURN g_avail_terr_tab_ams;
      ELSIF pi_app_cd = 'CICS_GP' THEN
        RETURN g_avail_terr_tab_cics_gp;
      ELSIF pi_app_cd = 'CSC' THEN
        RETURN g_avail_terr_tab_CSC;
      ELSIF pi_app_cd = 'MIS' THEN
        RETURN g_avail_terr_tab_MIS;
      ELSIF pi_app_cd = 'SVR' THEN
        RETURN g_avail_terr_tab_SVR;
      END IF;
      l_avail_terr_str := g_avail_terr_str_tab(pi_app_cd);
  
    END IF;

    RETURN l_list_tab;

  END get_avail_terr_tab;

  FUNCTION get_avail_terr_tab(pi_app_cd          IN        VARCHAR2
                             ) RETURN gp_type_str_tab
  IS
  BEGIN
    RETURN get_avail_terr_tab(USER,pi_app_cd);
  END get_avail_terr_tab;

  FUNCTION get_avail_terr_tab RETURN gp_type_str_tab
  IS
  BEGIN
    RETURN get_avail_terr_tab(USER,'CAS');
  END get_avail_terr_tab;

  FUNCTION is_legal_terr ( pi_terr_cd          IN            VARCHAR2
                          ,pi_user_id          IN            VARCHAR2
                          ,pi_app_cd           IN            VARCHAR2
                         ) RETURN VARCHAR2
  IS
    l_terr_tab                    gp_type_str_tab;
    l_exists                      VARCHAR2(1) := 'N';
  BEGIN
    RETURN l_exists;
  END is_legal_terr;

  FUNCTION is_legal_terr ( pi_terr_cd          IN            VARCHAR2
                          ,pi_app_cd           IN            VARCHAR2
                         ) RETURN VARCHAR2
  IS
  BEGIN
    RETURN is_legal_terr(pi_terr_cd,USER,pi_app_cd);
  END is_legal_terr;

  FUNCTION is_legal_terr ( pi_terr_cd          IN            VARCHAR2
                         ) RETURN VARCHAR2
  IS
  BEGIN
    RETURN is_legal_terr(pi_terr_cd,USER,'CAS');
  END is_legal_terr;


  FUNCTION get_ebs_agt_cd(pi_terr_cd          IN         VARCHAR2
                         ,pi_user_id          IN         VARCHAR2
                         ) RETURN VARCHAR2
  IS
  BEGIN
    RETURN 'abc';
  END get_ebs_agt_cd;

  FUNCTION get_ebs_agt_cd(pi_terr_cd          IN         VARCHAR2
                         ) RETURN VARCHAR2
  IS
  BEGIN
    RETURN get_ebs_agt_cd(pi_terr_cd,USER);
  END get_ebs_agt_cd;

  FUNCTION terr_cd RETURN VARCHAR2 IS
  BEGIN
    RETURN g_user_info_rec.terr_cd;
  END terr_cd;

  FUNCTION get_terr_cd_list_str(pi_app_cd         IN         VARCHAR2
                               ,pi_all_terr_ind   in         varchar2
                               ) RETURN VARCHAR2
  IS
  BEGIN
   RETURN 'a';
  END get_terr_cd_list_str;

  FUNCTION get_terr_cd_list_str(pi_app_cd         IN         VARCHAR2
                               ) RETURN VARCHAR2
  IS
  BEGIN
    IF g_avail_terr_str_tab.EXISTS(pi_app_cd) THEN
      RETURN g_avail_terr_str_tab(pi_app_cd);
    END IF;

    RETURN '';
  END get_terr_cd_list_str;

  FUNCTION get_terr_cd_list_str RETURN VARCHAR2
  IS
  BEGIN
    RETURN get_terr_cd_list_str('CAS');
  END get_terr_cd_list_str;

  FUNCTION is_menu_grant(pi_menu_cd  IN VARCHAR2
                        ,pi_app_cd   IN VARCHAR2)
            RETURN VARCHAR2
  IS
    c_curr_user_id  tuser_profiles.user_id%TYPE := USER;

    lv_return_ind   VARCHAR2(1);
  BEGIN

    lv_return_ind := 'N';

    BEGIN
       SELECT 'Y'
         INTO lv_return_ind
         FROM vuser_menus v
        WHERE v.user_id = c_curr_user_id
          AND v.menu_cd = pi_menu_cd
          AND v.app_cd = pi_app_cd
          AND ROWNUM = 1;
    EXCEPTION
      WHEN OTHERS
      THEN
        lv_return_ind := 'N';
    END;

    RETURN (lv_return_ind);

  END is_menu_grant;

  FUNCTION is_conn_ext_user(pi_user IN VARCHAR2) RETURN VARCHAR2 IS
   l_user tconn_ext_users.user_id%TYPE;

   CURSOR c_ext_user is
    SELECT user_id
      FROM tconn_ext_users
     WHERE rec_status = 'A'
       AND user_id = pi_user;
  BEGIN
   l_user := NUll;

   OPEN c_ext_user;
   FETCH c_ext_user INTO l_user;
   close c_ext_user;

   IF l_user IS NOT NULL THEN
    RETURN 'Y';
   ELSE
    RETURN 'N';
   END IF;
 END;
  FUNCTION get_user_terr_cd(pi_user_id          IN         VARCHAR2
                           ) RETURN varchar2
  IS
    CURSOR c_user_terr_cd IS
      SELECT terr_cd FROM tuser_profiles
       WHERE user_id = pi_user_id
       ;
    l_terr_cd                    tuser_profiles.terr_cd%TYPE;
  BEGIN
    OPEN c_user_terr_cd;
    FETCH c_user_terr_cd INTO l_terr_cd;
    CLOSE c_user_terr_cd;

    RETURN l_terr_cd;

  END get_user_terr_cd;
BEGIN
  init_pkg;
END cas_user;

//

CREATE OR REPLACE PACKAGE BODY "CTL_PARM"

IS

  TYPE type_parm_by_terr_rec IS RECORD (by_terr_ind       VARCHAR2(1)
                                        );

  TYPE type_parm_by_terr_tab IS TABLE OF type_parm_by_terr_rec INDEX BY VARCHAR2(30);

  TYPE type_parm_rec IS RECORD (  parm_valu tcontrol_parameters.parm_valu%TYPE
                                 ,data_typ  tcontrol_parameters.parm_typ%TYPE
                               );

  TYPE type_parm_tab IS TABLE OF type_parm_rec INDEX BY VARCHAR2(40);

  c_data_seperator                CONSTANT VARCHAR2(1) := ';';
  c_data_typ_char                 CONSTANT VARCHAR2(1) := 'C';
  c_data_typ_number               CONSTANT VARCHAR2(1) := 'N';
  c_data_typ_date                 CONSTANT VARCHAR2(1) := 'D';
  c_dflt_terr_cd                  CONSTANT VARCHAR2(1) := '1';
  c_ext_users           CONSTANT VARCHAR2(200) := 'SINO_MSP,'; -- 20171518

  g_parm_by_terr_tab              type_parm_by_terr_tab;
  g_parm_tbl_all                  type_parm_tab;
  g_dt_fmt                        VARCHAR2(20);
  l_key                           VARCHAR2(40);


  


  FUNCTION get_key(pi_parm_typ       IN       VARCHAR2
                  ,pi_terr_cd        IN       VARCHAR2
                  ,pi_crcy_code      IN       VARCHAR2  DEFAULT '*'
                  ) RETURN VARCHAR2
  IS
  BEGIN
    RETURN pi_parm_typ
          ||c_data_seperator||pi_terr_cd
          ||c_data_seperator||pi_crcy_code
          ;
  END get_key;

  PROCEDURE init_pkg
  IS
    l_key                         VARCHAR2(40);
  BEGIN
    g_parm_tbl_all.DELETE;
    g_parm_by_terr_tab.DELETE;

    FOR rec IN (SELECT *
                  FROM tcontrol_parameters
                ) LOOP
      l_key := get_key(rec.parm_typ
                      ,rec.terr_cd
                      ,rec.crcy_code
                      );
      g_parm_tbl_all(l_key).parm_valu := rec.parm_valu;
      g_parm_tbl_all(l_key).data_typ := rec.data_typ;

      IF rec.terr_cd = c_dflt_terr_cd THEN
        g_parm_by_terr_tab(rec.parm_typ).by_terr_ind := 'N';
      ELSE
        g_parm_by_terr_tab(rec.parm_typ).by_terr_ind := 'Y';
      END IF;

      IF rec.parm_typ = 'DT_FMT' THEN
        g_dt_fmt := rec.parm_valu;
      END IF;

    END LOOP;

  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('init_pkg error');
      DBMS_OUTPUT.PUT_LINE(sqlerrm);
  END init_pkg;

  PROCEDURE init_parm IS
  BEGIN
    init_pkg; 
              
  END init_parm;

  FUNCTION get_parm_valu(pi_parm_typ       IN       VARCHAR2
                        ,pi_terr_cd        IN       VARCHAR2    DEFAULT NULL
                        ,pi_crcy_code      IN       VARCHAR2    DEFAULT '*'
                        ) RETURN VARCHAR2
  IS
    l_key                         VARCHAR2(40);
    l_terr_cd                     tcontrol_parameters.terr_cd%TYPE;


    l_ext_parm_valu  VARCHAR2(30);
    CURSOR c_ext_user_parm(pi_user_id IN VARCHAR2) IS
     SELECT parm_valu
       FROM tconn_ext_user_ctl_parm
      WHERE user_id = pi_user_id
        AND parm_typ = pi_parm_typ
        AND rec_status = 'A';

  BEGIN
  
   IF pi_parm_typ = 'CAS_DT' THEN
    IF USER IN ('SINO_MSP', 'CAS_MQ')THEN
     do.pl('Return ext user parm');
     OPEN c_ext_user_parm(USER);
     FETCH c_ext_user_parm INTO l_ext_parm_valu;
     CLOSE c_ext_user_parm;

     IF l_ext_parm_valu IS NOT NULL THEN
      do.pl('Return ext user CAS_DT = ' || to_char(l_ext_parm_valu));
      RETURN l_ext_parm_valu;
     END IF;
    END IF;
   END IF;

    IF g_parm_by_terr_tab.EXISTS(pi_parm_typ)
      AND g_parm_by_terr_tab(pi_parm_typ).by_terr_ind = 'Y'
    THEN
      l_terr_cd := NVL(pi_terr_cd,terr_fcn.get_terr_cd);
    ELSE
      l_terr_cd := c_dflt_terr_cd;
    END IF;

    l_key := get_key(pi_parm_typ,l_terr_cd,pi_crcy_code);

    IF g_parm_tbl_all.EXISTS(l_key) THEN
      RETURN g_parm_tbl_all(l_key).parm_valu;
    END IF;

    RETURN NULL;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END get_parm_valu;

 
  PROCEDURE get(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
               ,po_rtrn_valu OUT VARCHAR2
               )
  IS
  BEGIN
    po_rtrn_valu := get_parm_valu(pi_parm_typ);
  END get;

  PROCEDURE get(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
               ,po_rtrn_valu OUT NUMBER
               )
  IS
    l_parm_valu                   tcontrol_parameters.parm_valu%TYPE;
    x_err_no_parm                 EXCEPTION;
  BEGIN
    get(pi_parm_typ           => pi_parm_typ
       ,po_rtrn_valu          => l_parm_valu
       );
    IF l_parm_valu IS NOT NULL THEN
      po_rtrn_valu := TO_NUMBER(l_parm_valu);
    ELSE
      RAISE x_err_no_parm;
    END IF;
  EXCEPTION
    WHEN x_err_no_parm THEN
      po_rtrn_valu := 0; 
    WHEN others THEN
      po_rtrn_valu := NULL;
  END get;

  PROCEDURE get(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
               ,po_rtrn_valu OUT DATE
               )
  IS
    l_parm_valu                   tcontrol_parameters.parm_valu%TYPE;
  BEGIN
    get(pi_parm_typ           => pi_parm_typ
       ,po_rtrn_valu          => l_parm_valu
       );
    po_rtrn_valu := TO_DATE(l_parm_valu,g_dt_fmt);

    IF pi_parm_typ = 'CAS_DT' THEN
      IF USER = 'CCNT' THEN
        po_rtrn_valu := cas_sys.cas_dt;
      ELSIF FCN.Redo_dt IS NOT NULL THEN
        po_rtrn_valu := FCN.Redo_Dt;
      END IF;
    END IF;

  EXCEPTION
    WHEN others THEN
      po_rtrn_valu := NULL;
  END get;

  PROCEDURE get_real(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
                    ,po_rtrn_valu OUT DATE
                    )
  IS
  BEGIN
    po_rtrn_valu := TO_DATE(get_parm_valu(pi_parm_typ),g_dt_fmt);
  EXCEPTION
    WHEN others THEN
      po_rtrn_valu := NULL;
  END get_real;

  PROCEDURE chng(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
                ,pi_set_valu  IN  VARCHAR2
                )
  IS
    l_key                         VARCHAR2(40);
    l_terr_cd                     tcontrol_parameters.terr_cd%TYPE;
  BEGIN
    IF g_parm_by_terr_tab.EXISTS(pi_parm_typ)
      AND g_parm_by_terr_tab(pi_parm_typ).by_terr_ind = 'Y'
    THEN
      l_terr_cd := terr_fcn.get_terr_cd;
    ELSE
      l_terr_cd := c_dflt_terr_cd;
    END IF;
    l_key := get_key(pi_parm_typ,l_terr_cd);

    IF g_parm_tbl_all.EXISTS(l_key) THEN
      g_parm_tbl_all(l_key).parm_valu := pi_set_valu;
    END IF;
  END chng;

  PROCEDURE chng(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
                ,pi_set_valu  IN  NUMBER
                )
  IS
    l_key                         VARCHAR2(40);
    l_terr_cd                     tcontrol_parameters.terr_cd%TYPE;
  BEGIN
    IF g_parm_by_terr_tab.EXISTS(pi_parm_typ)
      AND g_parm_by_terr_tab(pi_parm_typ).by_terr_ind = 'Y'
    THEN
      l_terr_cd := terr_fcn.get_terr_cd;
    ELSE
      l_terr_cd := c_dflt_terr_cd;
    END IF;
    l_key := get_key(pi_parm_typ,l_terr_cd);

    IF g_parm_tbl_all.EXISTS(l_key) THEN
      g_parm_tbl_all(l_key).parm_valu := TO_CHAR(pi_set_valu);
    END IF;
  END chng;

  PROCEDURE chng(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
                ,pi_set_valu  IN  DATE
                )
  IS
    l_key                         VARCHAR2(40);
    l_terr_cd                     tcontrol_parameters.terr_cd%TYPE;
  BEGIN
    IF g_parm_by_terr_tab.EXISTS(pi_parm_typ)
      AND g_parm_by_terr_tab(pi_parm_typ).by_terr_ind = 'Y'
    THEN
      l_terr_cd := terr_fcn.get_terr_cd;
    ELSE
      l_terr_cd := c_dflt_terr_cd;
    END IF;
    l_key := get_key(pi_parm_typ,l_terr_cd);

    IF g_parm_tbl_all.EXISTS(l_key) THEN
      g_parm_tbl_all(l_key).parm_valu := TO_CHAR(pi_set_valu,g_dt_fmt);
    END IF;
  END chng;

  PROCEDURE get(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
               ,pi_crcy_code IN  tcurrency_master.crcy_code%type
               ,po_rtrn_valu OUT VARCHAR2
               )
  IS
    l_parm_valu                   tcontrol_parameters.parm_valu%TYPE;
    l_terr_cd                     tcontrol_parameters.terr_cd%TYPE;
  BEGIN
    l_parm_valu := get_parm_valu(pi_parm_typ          => pi_parm_typ
                                ,pi_crcy_code         => pi_crcy_code
                                );
    IF l_parm_valu IS NOT NULL THEN
      po_rtrn_valu := l_parm_valu;
      RETURN;
    END IF;

   
    l_parm_valu := get_parm_valu(pi_parm_typ,NULL,'*');
    po_rtrn_valu := l_parm_valu;
  END get;

  PROCEDURE get(pi_parm_typ  IN  tcontrol_parameters.parm_typ%TYPE
               ,pi_crcy_code IN  tcurrency_master.crcy_code%type
               ,po_rtrn_valu OUT NUMBER
               )
  IS
    l_parm_valu                   tcontrol_parameters.parm_valu%TYPE;
  BEGIN
    get(pi_parm_typ         => pi_parm_typ
       ,pi_crcy_code        => pi_crcy_code
       ,po_rtrn_valu        => l_parm_valu
       );
    po_rtrn_valu := TO_NUMBER(l_parm_valu);
  EXCEPTION
    WHEN others THEN
      po_rtrn_valu := 0;
  END get;


  PROCEDURE get(pi_parm_typ  IN  VARCHAR2
               ,pi_crcy_code IN  VARCHAR2
               ,pi_plan_code IN  VARCHAR2
               ,pi_eff_dt    IN  DATE
               ,po_rtrn_valu OUT VARCHAR2
               ,pi_vers_num  IN  VARCHAR2  DEFAULT NULL
               )
  IS
    l_parm_valu                   tcontrol_parameters.parm_valu%TYPE;
	CURSOR c_plan_parm (p_parm_typ     VARCHAR2
                     ,p_plan_code    VARCHAR2
                     ,p_vers_num     VARCHAR2
                     ,p_eff_dt       date
                     ) IS
    SELECT parm_valu
      FROM tplan_parameters
     WHERE parm_typ  = p_parm_typ
       AND plan_code = p_plan_code
       AND vers_num  = nvl(p_vers_num,vers_num)   -- 2007-003
       AND eff_dt   <= p_eff_dt
     ORDER BY eff_dt desc;
  BEGIN
    OPEN c_plan_parm (pi_parm_typ, pi_plan_code, pi_vers_num,pi_eff_dt);
    FETCH c_plan_parm INTO l_parm_valu;
    IF c_plan_parm%FOUND THEN
       CLOSE c_plan_parm;
    ELSE
       CLOSE c_plan_parm;
       get(pi_parm_typ,pi_crcy_code,l_parm_valu);
    END IF;
    po_rtrn_valu := l_parm_valu;
  END get;

  PROCEDURE get(pi_parm_typ  IN  VARCHAR2
               ,pi_crcy_code IN  VARCHAR2
               ,pi_plan_code IN  VARCHAR2
               ,pi_eff_dt    IN  DATE
               ,po_rtrn_valu OUT NUMBER
               ,pi_vers_num  IN  VARCHAR2  DEFAULT NULL
               )
  IS
    l_parm_valu                   tcontrol_parameters.parm_valu%TYPE;
  BEGIN
    get(pi_parm_typ           => pi_parm_typ
       ,pi_crcy_code          => pi_crcy_code
       ,pi_plan_code          => pi_plan_code
       ,pi_eff_dt             => pi_eff_dt
       ,po_rtrn_valu          => l_parm_valu
       ,pi_vers_num           => pi_vers_num
       );
    po_rtrn_valu := TO_NUMBER(l_parm_valu);
  END get;
  
  FUNCTION get_char(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                   ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                   ,pi_crcy_code    IN  tcontrol_parameters.crcy_code%TYPE
                   ) RETURN VARCHAR2
  IS
  BEGIN
    RETURN get_parm_valu(pi_parm_typ        => pi_parm_typ
                        ,pi_terr_cd         => pi_terr_cd
                        ,pi_crcy_code       => pi_crcy_code
                        );
  END get_char;


  FUNCTION get_char(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                   ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                   ) RETURN VARCHAR2
  IS
  BEGIN
    RETURN get_char(pi_parm_typ,pi_terr_cd,'*');
  END get_char;

  FUNCTION get_char(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                   ) RETURN VARCHAR2
  IS
  BEGIN
    RETURN get_char(pi_parm_typ,NULL,'*');
  END get_char;


  FUNCTION get_dt(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                 ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                 ,pi_crcy_code    IN  tcontrol_parameters.crcy_code%TYPE
                 ) RETURN DATE
  IS
    l_parm_valu                   tcontrol_parameters.parm_valu%TYPE;
  BEGIN
    l_parm_valu := get_parm_valu(pi_parm_typ        => pi_parm_typ
                                ,pi_terr_cd         => pi_terr_cd
                                ,pi_crcy_code       => pi_crcy_code
                                );

    RETURN TO_DATE(l_parm_valu ,g_dt_fmt);
  END get_dt;

  FUNCTION get_dt(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                 ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                 ) RETURN DATE
  IS
  BEGIN
    RETURN get_dt(pi_parm_typ,pi_terr_cd,'*');
  END get_dt;

  FUNCTION get_dt(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                 ) RETURN DATE
  IS
  BEGIN
    RETURN get_dt(pi_parm_typ,NULL,'*');
  END get_dt;


  FUNCTION get_num(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                  ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                  ,pi_crcy_code    IN  tcontrol_parameters.crcy_code%TYPE
                  ) RETURN NUMBER
  IS
    l_parm_valu                   tcontrol_parameters.parm_valu%TYPE;
  BEGIN
    l_parm_valu := get_parm_valu(pi_parm_typ        => pi_parm_typ
                                ,pi_terr_cd         => pi_terr_cd
                                ,pi_crcy_code       => pi_crcy_code
                                );

    RETURN TO_NUMBER(l_parm_valu);
  END get_num;

  FUNCTION get_num(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                  ,pi_terr_cd      IN  tcontrol_parameters.terr_cd%TYPE
                  ) RETURN NUMBER
  IS
  BEGIN
    RETURN get_num(pi_parm_typ,pi_terr_cd,'*');
  END get_num;

  FUNCTION get_num(pi_parm_typ     IN  tcontrol_parameters.parm_typ%TYPE
                  ) RETURN NUMBER
  IS
  BEGIN
    RETURN get_num(pi_parm_typ,NULL,'*');
  END get_num;
BEGIN
  init_pkg;
END ctl_parm;
//

CREATE OR REPLACE PACKAGE BODY "DO"
IS

 show_output_flag BOOLEAN := TRUE;

 PROCEDURE display_line
  (show_in IN VARCHAR2, line_in IN VARCHAR2)
 IS
                t_OUTPUT TBL_OUTPUT;
                l_debug_trace  VARCHAR2(10) := 'ON'; 
 BEGIN
  IF show_output_flag OR UPPER (show_in) = 'SHOW'
  THEN
    IF NVL ( l_debug_trace, 'OFF' ) = 'ON' THEN 
          if length(line_in) < 256 then
           DBMS_OUTPUT.PUT_LINE (line_in);
                 else
                         t_OUTPUT := wrap(line_in);
                         for i in 1 .. t_output.count loop
                                 dbms_output.put_line(t_output(i));
                         end loop;
                 end if;
           end if; 
  END IF;
 END;
 FUNCTION boolean_string
  (boolean_in IN BOOLEAN, char_in IN VARCHAR2 := NULL)
 RETURN VARCHAR2
 IS
 BEGIN
  IF boolean_in
  THEN
   RETURN char_in || ' ' || 'TRUE';
  ELSE
   RETURN char_in || ' ' || 'FALSE';
  END IF;
 END;
 
 PROCEDURE switch (show_in IN VARCHAR2 := NULL)

 IS
 BEGIN
  IF show_in IS NULL
  THEN
   show_output_flag := NOT show_output_flag;
  ELSE
   show_output_flag := UPPER (show_in) = 'ON';
  END IF;
 END;
 
 PROCEDURE pl
  (date_in IN DATE,
   mask_in IN VARCHAR2 := 'Month DD, YYYY - HH:MI:SS PM',
   show_in IN VARCHAR2 := NULL)
 IS
 BEGIN
  display_line (show_in, TO_CHAR (date_in, mask_in));
 END;
 PROCEDURE pl (number_in IN NUMBER, show_in IN VARCHAR2 := NULL)
 IS
 BEGIN
  display_line (show_in, number_in);
 END;
        PROCEDURE pl (hint_in IN VARCHAR2, char_in IN VARCHAR2 := NULL, show_in IN VARCHAR2 := NULL)
        IS
          l_char_in VARCHAR2(2000);
        BEGIN
                if char_in is not null then
                   l_char_in := ' : ' || char_in;
                end if;
                display_line (show_in, hint_in || l_char_in);
        END;
 PROCEDURE pl
  (char_in IN VARCHAR2, number_in IN NUMBER, show_in IN VARCHAR2 := NULL)
 IS
 BEGIN
  display_line
   (show_in, char_in || ': ' || TO_CHAR (number_in));
 END;
 PROCEDURE pl
  (char_in IN VARCHAR2,
   date_in IN DATE,
   mask_in IN VARCHAR2 := 'Month DD, YYYY - HH:MI:SS PM',
   show_in IN VARCHAR2 := NULL)
 IS
 BEGIN
  display_line
   (show_in, char_in || ': ' || TO_CHAR (date_in, mask_in));
 END;
 PROCEDURE pl (boolean_in IN BOOLEAN, show_in IN VARCHAR2 := NULL)

 IS
 BEGIN
  display_line (show_in, boolean_string (boolean_in));
 END;
 PROCEDURE pl
  (char_in IN VARCHAR2,
   boolean_in IN BOOLEAN,
   show_in IN VARCHAR2 := NULL)
 IS
 BEGIN
  display_line (show_in, boolean_string (boolean_in, char_in));
 END;
        FUNCTION WRAP(text_in      IN VARCHAR2
                     ,line_length  IN INTEGER  := 255
                     ,delimited_by IN VARCHAR2 := ' '
                     )
        RETURN TBL_OUTPUT
        IS
  len_text       INTEGER := LENGTH (text_in);
  line_start_loc INTEGER := 1;
  line_end_loc   INTEGER := 1;
  last_space_loc INTEGER;
         num_lines_out  INTEGER;
  curr_line      VARCHAR2(2000);
                l_output       TBL_OUTPUT;
        BEGIN
  IF len_text IS NULL
  THEN
   RETURN l_output;
  ELSE
   num_lines_out := 1;
   LOOP
    EXIT WHEN line_end_loc > len_text;
    line_end_loc := LEAST (line_end_loc + line_length, len_text + 1);
   
    curr_line := SUBSTR (text_in || delimited_by, line_start_loc, line_length + 1);
  
    last_space_loc    := INSTR (curr_line, delimited_by, -1);
    if last_space_loc = 0 then
       last_space_loc := instr(curr_line,' ',-1);
    end if;
    if last_space_loc = 0 then
       last_space_loc := INSTR (curr_line, ',', -1);
    end if;
                                if last_space_loc = 0 then
                                   last_space_loc := line_length;
                                end if;
                                
    line_end_loc := line_start_loc + last_space_loc;
  
    l_output (num_lines_out) := substr (text_in,line_start_loc,line_end_loc - line_start_loc);
   
    num_lines_out := num_lines_out + 1;
    line_start_loc := line_end_loc;
   END LOOP;
   num_lines_out := num_lines_out - 1;
  END IF;
  RETURN l_output;
        END WRAP;
begin
   dbms_output.enable(1000000);
END do;
//
CREATE OR REPLACE PACKAGE BODY "FCN"
IS
l_Redo_Dt     date := NULL;
Procedure Set_Redo_Dt(pi_redo_dt in date)
  is
  begin
      l_Redo_Dt := pi_redo_dt;
  end Set_Redo_Dt;
  
    Function  Redo_Dt
    return  date
  is
  begin
    return l_Redo_Dt;
  end Redo_Dt;
END;
//
CREATE OR REPLACE PACKAGE BODY "TERR_FCN" is
    v_terr_cd        tuser_profiles.terr_cd%type;
    v_terr_cd_withoutHO        tuser_profiles.terr_cd%type;   
    v_avail_terrs    varchar2(2000);                   
    v_avail_terrs_withoutHO  varchar2(2000);           
   
    v_app_cd         tapp_menus.app_cd%type; 
    procedure set_terr_cd(pi_terr_cd in varchar2) is
    begin

        if pi_terr_cd is null then
            v_terr_cd   := sf_get_terr_cd(user);
        else
            v_terr_cd   := pi_terr_cd;
        end if;

    end set_terr_cd;
    function get_terr_cd return varchar2 is
    begin

        if v_terr_cd is null then
            return sf_get_terr_cd(user);
        else
            return v_terr_cd;
        end if;

    end get_terr_cd;

    function get_terr_cd_withoutHO return varchar2 is
    begin
       v_terr_cd_withoutHO := get_terr_cd();   
       if v_terr_cd_withoutHO in('H1') then    
          v_terr_cd_withoutHO := sf_get_terr_cd(user);
       end if;
       return v_terr_cd_withoutHO;
    end get_terr_cd_withoutHO;

    procedure set_avail_terrs( pi_user_id in varchar2)
    is
    begin

        IF pi_user_id = USER THEN
          v_avail_terrs := cas_user.get_terr_cd_list_str(nvl(v_app_cd, '*'));
      
        END IF;  
    end set_avail_terrs;

    function get_avail_terrs return varchar2 is
    begin
        if v_avail_terrs is null then
            v_avail_terrs := cas_user.get_terr_cd_list_str(nvl(v_app_cd, '*'));
           
        end if;
        return v_avail_terrs;
    end get_avail_terrs;

  function get_avail_terrs_withoutHO return varchar2 is
  begin
     if v_avail_terrs_withoutHO is null then
        v_avail_terrs_withoutHO := terr_fcn.get_avail_terrs;
     end if;
     v_avail_terrs_withoutHO := replace(v_avail_terrs_withoutHO,'H1/',null);
     return v_avail_terrs_withoutHO;
  end get_avail_terrs_withoutHO;

    procedure set_avail_terrs( pi_user_id in varchar2
                              ,pi_all_terr_ind in varchar2
                              )IS
    BEGIN

        IF pi_user_id = USER THEN
          v_avail_terrs := cas_user.get_terr_cd_list_str(nvl(v_app_cd, '*'),nvl(pi_all_terr_ind,'N'));
     
        END IF;  
    end set_avail_terrs;
    function get_avail_terrs (pi_all_terr_ind in varchar2) return varchar2 is
    begin
        if v_avail_terrs is null then
            v_avail_terrs := cas_user.get_terr_cd_list_str(nvl(v_app_cd, '*'),nvl(pi_all_terr_ind,'N'));  --  NB Rewrite
        end if;
        return v_avail_terrs;
    end get_avail_terrs;

    procedure terr_switch(pi_terr_cd in varchar2) is
    begin
        if v_terr_cd is null then
            v_terr_cd   := pi_terr_cd;
            ctl_parm.init_parm;
        elsif v_terr_cd <> pi_terr_cd then
            v_terr_cd   := pi_terr_cd;
            ctl_parm.init_parm;
        end if;
    end terr_switch;
--
--   set current application code
--
    procedure set_app_cd(pi_app_cd in varchar2) is
    begin
        v_app_cd    := pi_app_cd;
    end set_app_cd;
--
--   get current application code
--
    function get_app_cd return varchar2 is
    begin
        return v_app_cd;
    end get_app_cd;
-- << 2005-br-01 end
    -- <<20140847
    function get_all_terrs return varchar2 is
    begin
     return '';
    end get_all_terrs;
end;
//
CREATE OR REPLACE TRIGGER "AF_CPL_UPD"
after insert or update or delete of BANK_ACCT_typ
ON TCLIENT_POLICY_LINKS
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
declare
 l_dt   date := get_cas_sys_dt;
 l_col_nm   varchar2(20) := 'BANK_ACCT_TYP';
 c_action_cd_insert constant varchar2(1)  := 'I';
 c_action_cd_update constant varchar2(1)  := 'U';
 c_action_cd_delete constant varchar2(1)  := 'D';
 l_rslt_seq number;
 l_ipa_acct_num tclient_bank_accounts.bank_acct_num%type;
 l_ipa_bank_cd   tclient_bank_accounts.bank_cd%type;
 l_new_acct_num tclient_bank_accounts.bank_acct_num%type;
 l_new_bank_cd   tclient_bank_accounts.bank_cd%type;
 l_old_acct_num tclient_bank_accounts.bank_acct_num%type;
 l_old_bank_cd   tclient_bank_accounts.bank_cd%type;
 l_del_acct_num tclient_bank_accounts.bank_acct_num%type;
 l_del_bank_cd   tclient_bank_accounts.bank_cd%type;
 l_audit_log_rec tcpl_audit_log%rowtype;
 procedure get_ipa_acct (pi_pol_num in varchar2,
             po_bank_acct_num out varchar2,
             po_bank_cd out varchar2)
  is
  cursor cur_ipa is
    select bank_acct_num,po_bank_cd
      from tipa_reg_info
     where pol_num = pi_pol_num;
  begin
   open cur_ipa;
   fetch cur_ipa into po_bank_acct_num,po_bank_cd;
    close cur_ipa;
 end;
 procedure get_acct_num (pi_cli_num in varchar2,
             pi_acct_typ in varchar2,
             po_bank_acct_num out varchar2,
             po_bank_cd out varchar2)
  is
  cursor cur_acct_num  is
   select bank_acct_num,bank_cd
     from tclient_bank_accounts
    where cli_num = pi_cli_num
      and bank_acct_typ = pi_acct_typ;
  begin
  open cur_acct_num;
  fetch cur_acct_num into po_bank_acct_num,po_bank_cd;
  close cur_acct_num;
 end;
 procedure insert_audit_log(pi_audit_log_rec tcpl_audit_log%rowtype)
 is
 begin
  insert into tcpl_audit_log values pi_audit_log_rec;
 end;
 -- <<2009-214
 procedure get_del_acct(pi_pol_num in varchar2,
                        po_bank_acct_num out varchar2,
                        po_bank_cd out varchar2)
 is
   cursor c_del is
     select old_valu,old_valu_ref
       from tcpl_audit_log
      where trxn_dt = l_dt
        and action_cd = 'D'
        and pol_num = pi_pol_num
        and link_typ = 'O'
       order by cpl_atr_num desc;
 begin
   open c_del;
   fetch c_del into po_bank_acct_num,po_bank_cd;
   close c_del;
 end;
  -- >>2009-214
begin
 if :new.link_typ = 'O' then
   if (inserting) then
      get_del_acct(:new.pol_num,l_del_acct_num,l_del_bank_cd);-- 2009-214
      get_acct_num(:new.cli_num,:new.bank_acct_typ,l_new_acct_num,l_new_bank_cd);  -- 2009-214
      if l_del_acct_num is null then -- 2009-214
        get_ipa_acct(:new.pol_num,l_ipa_acct_num,l_ipa_bank_cd);
       -- 2009-214 get_acct_num(:new.cli_num,:new.bank_acct_typ,l_new_acct_num,l_new_bank_cd);
        if l_ipa_acct_num is null then
          select cpl_atr_num_seq.nextval
          into l_rslt_seq
          from dual;
          l_audit_log_rec.pol_num           :=   :new.pol_num;
          l_audit_log_rec.cli_num           :=   :new.cli_num;
          l_audit_log_rec.link_typ          :=   :new.link_typ;
          l_audit_log_rec.col_nm            :=   l_col_nm;
          l_audit_log_rec.trxn_dt           :=   l_dt;
          l_audit_log_rec.cpl_atr_num       :=   l_rslt_seq;
          l_audit_log_rec.old_valu          :=   null;
          l_audit_log_rec.old_valu_ref      :=   null;
          l_audit_log_rec.new_valu          :=   l_new_acct_num;
          l_audit_log_rec.new_valu_ref      :=   l_new_bank_cd ;
          l_audit_log_rec.terr_cd           :=   get_terr_cd(:new.pol_num);
          l_audit_log_rec.old_valu_expand   :=  :old.cli_num;
          l_audit_log_rec.user_id           :=  user;
          l_audit_log_rec.action_cd         :=  c_action_cd_insert;
          insert_audit_log(l_audit_log_rec);
        elsif l_new_acct_num is null then
          select cpl_atr_num_seq.nextval
          into l_rslt_seq
          from dual;
          l_audit_log_rec.pol_num           :=   :new.pol_num;
          l_audit_log_rec.cli_num           :=   :new.cli_num;
          l_audit_log_rec.link_typ          :=   :new.link_typ;
          l_audit_log_rec.col_nm            :=   l_col_nm;
          l_audit_log_rec.trxn_dt           :=   l_dt;
          l_audit_log_rec.cpl_atr_num       :=   l_rslt_seq;
          l_audit_log_rec.old_valu          :=   l_ipa_acct_num;
          l_audit_log_rec.old_valu_ref      :=   l_ipa_bank_cd;
          l_audit_log_rec.new_valu          :=   null;
          l_audit_log_rec.new_valu_ref      :=   null ;
          l_audit_log_rec.terr_cd           :=   get_terr_cd(:new.pol_num);
          l_audit_log_rec.old_valu_expand   :=  :old.cli_num;
          l_audit_log_rec.user_id           :=  user;
          l_audit_log_rec.action_cd         :=  c_action_cd_insert;
          insert_audit_log(l_audit_log_rec);
        else
          if (l_ipa_acct_num <> l_new_acct_num )
             or
             (l_ipa_bank_cd <> l_new_bank_cd ) then
            select cpl_atr_num_seq.nextval
            into l_rslt_seq
            from dual;
            l_audit_log_rec.pol_num           :=   :new.pol_num;
            l_audit_log_rec.cli_num           :=   :new.cli_num;
            l_audit_log_rec.link_typ          :=   :new.link_typ;
            l_audit_log_rec.col_nm            :=   l_col_nm;
            l_audit_log_rec.trxn_dt           :=   l_dt;
            l_audit_log_rec.cpl_atr_num       :=   l_rslt_seq;
            l_audit_log_rec.old_valu          :=   l_ipa_acct_num;
            l_audit_log_rec.old_valu_ref      :=   l_ipa_bank_cd;
            l_audit_log_rec.new_valu          :=   l_new_acct_num;
            l_audit_log_rec.new_valu_ref      :=   l_new_bank_cd ;
            l_audit_log_rec.terr_cd           :=   get_terr_cd(:new.pol_num);
            l_audit_log_rec.old_valu_expand   :=  :old.cli_num;
            l_audit_log_rec.user_id           :=  user;
            l_audit_log_rec.action_cd         :=  c_action_cd_insert;
            insert_audit_log(l_audit_log_rec);
          end if;
        end if;
      elsif l_del_acct_num <> nvl(l_new_acct_num,'*')
            or l_del_bank_cd <> nvl(l_new_bank_cd,'*') then
        select cpl_atr_num_seq.nextval
        into l_rslt_seq
        from dual;
        l_audit_log_rec.pol_num           :=   :new.pol_num;
        l_audit_log_rec.cli_num           :=   :new.cli_num;
        l_audit_log_rec.link_typ          :=   :new.link_typ;
        l_audit_log_rec.col_nm            :=   l_col_nm;
        l_audit_log_rec.trxn_dt           :=   l_dt;
        l_audit_log_rec.cpl_atr_num       :=   l_rslt_seq;
        l_audit_log_rec.old_valu          :=   l_del_acct_num;
        l_audit_log_rec.old_valu_ref      :=   l_del_bank_cd;
        l_audit_log_rec.new_valu          :=   l_new_acct_num;
        l_audit_log_rec.new_valu_ref      :=   l_new_bank_cd ;
        l_audit_log_rec.terr_cd           :=   get_terr_cd(:new.pol_num);
        l_audit_log_rec.old_valu_expand   :=   :old.cli_num;
        l_audit_log_rec.user_id           :=   user;
        l_audit_log_rec.action_cd         :=  c_action_cd_insert;
        insert_audit_log(l_audit_log_rec);
      end if;
   elsif (updating) then
     get_acct_num(:old.cli_num,:old.bank_acct_typ,l_old_acct_num,l_old_bank_cd);
     get_acct_num(:new.cli_num,:new.bank_acct_typ,l_new_acct_num,l_new_bank_cd);
   
     if NVL(:old.bank_acct_typ,'0')<> NVL(:new.bank_acct_typ,'0')
        and :old.cli_num = :new.cli_num  then
       select cpl_atr_num_seq.nextval
       into l_rslt_seq
       from dual;
       l_audit_log_rec.pol_num        :=   :new.pol_num;
       l_audit_log_rec.cli_num           :=   :new.cli_num;
       l_audit_log_rec.link_typ          :=   :new.link_typ;
       l_audit_log_rec.col_nm            :=   l_col_nm;
       l_audit_log_rec.trxn_dt           :=   l_dt;
       l_audit_log_rec.cpl_atr_num       :=   l_rslt_seq;
       l_audit_log_rec.old_valu          :=   l_old_acct_num;
       l_audit_log_rec.old_valu_ref     :=   l_old_bank_cd;
       l_audit_log_rec.new_valu          :=   l_new_acct_num;
       l_audit_log_rec.new_valu_ref     :=   l_new_bank_cd ;
       l_audit_log_rec.terr_cd           :=   get_terr_cd(:new.pol_num);
       l_audit_log_rec.old_valu_expand   :=  :old.cli_num;
       l_audit_log_rec.user_id           :=  user;
       l_audit_log_rec.action_cd           :=  c_action_cd_update;
       insert_audit_log(l_audit_log_rec);
     end if;
   end if;
 else
   if (deleting) and :old.link_typ = 'O' then
     get_acct_num(:old.cli_num,:old.bank_acct_typ,l_old_acct_num,l_old_bank_cd);
     if  l_old_acct_num is not null then
       select cpl_atr_num_seq.nextval
       into l_rslt_seq
       from dual;
       l_audit_log_rec.pol_num           :=   :old.pol_num;
       l_audit_log_rec.cli_num           :=   :old.cli_num;
       l_audit_log_rec.link_typ          :=   :old.link_typ;
       l_audit_log_rec.col_nm            :=   l_col_nm;
       l_audit_log_rec.trxn_dt           :=   l_dt;
       l_audit_log_rec.cpl_atr_num       :=   l_rslt_seq;
       l_audit_log_rec.old_valu          :=   l_old_acct_num;
       l_audit_log_rec.old_valu_ref      :=   l_old_bank_cd;
       l_audit_log_rec.new_valu          :=   null;
       l_audit_log_rec.new_valu_ref      :=   null ;
       l_audit_log_rec.terr_cd           :=   get_terr_cd(:old.pol_num);
       l_audit_log_rec.old_valu_expand   :=  :old.cli_num;
       l_audit_log_rec.user_id           :=  user;
       l_audit_log_rec.action_cd         :=  c_action_cd_delete;
       insert_audit_log(l_audit_log_rec);
     end if;
   end if;
 end if;
end;
//
CREATE OR REPLACE TRIGGER "AF_LAST_PRINTED_UPD"
after update of last_printed
ON TPRINT_CONTROLS
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
declare

c_fcn_nm  CONSTANT VARCHAR2(30) := 'CASRCM10SH_LOG';
l_switch tcontrol_parameters.parm_valu%type;
begin

   l_switch := 'Y';
  if l_switch = 'Y' and :OLD.fcn_id = 'CASRCM10SH' then
    cas_sys.reg_sys_log(pi_fcn_nm         => c_fcn_nm
               ,pi_pol_num        => :NEW.pol_num
               ,pi_log_typ        => 'I'
               ,pi_fcn_return     => 0
               ,pi_sqlerrm        => 'UPDATE USER: ' || USER || ',TIME: ' || to_char(SYSDATE,'YYYY/MM/DD HH:MM:SS')
                );
  end if;

end;

//
delimiter ;//




