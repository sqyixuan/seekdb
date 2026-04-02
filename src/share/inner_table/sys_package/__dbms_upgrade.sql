#package_name: __dbms_upgrade
#author: linlin.xll

CREATE OR REPLACE PACKAGE "__DBMS_UPGRADE" IS
  PROCEDURE UPGRADE(package_name VARCHAR2,
                    load_from_file BOOLEAN DEFAULT TRUE);
  PROCEDURE UPGRADE_ALL(load_from_file BOOLEAN DEFAULT TRUE);
  PROCEDURE FLUSH_DLL_NCOMP;
END;
//
