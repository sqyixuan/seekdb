#package_name:dbms_lob
#author: xiaofeng.lby

CREATE OR REPLACE PACKAGE BODY DBMS_LOB AS
  FUNCTION ISOPEN (
     lob_loc IN BLOB) 
    RETURN INTEGER; 
  PRAGMA INTERFACE(c, dbms_lob_isopen);

  FUNCTION ISOPEN (
     lob_loc IN CLOB CHARACTER SET ANY_CS) 
    RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_lob_isopen);
  
  PROCEDURE OPEN (
     lob_loc   IN OUT NOCOPY BLOB,
     open_mode IN            BINARY_INTEGER);
  PRAGMA INTERFACE(c, dbms_lob_open);
   
  PROCEDURE OPEN (
     lob_loc   IN OUT NOCOPY CLOB CHARACTER SET ANY_CS,
     open_mode IN            BINARY_INTEGER);
  PRAGMA INTERFACE(c, dbms_lob_open);

  PROCEDURE CLOSE (
     lob_loc    IN OUT NOCOPY BLOB); 
  PRAGMA INTERFACE(c, dbms_lob_close);

  PROCEDURE CLOSE (
     lob_loc    IN OUT NOCOPY CLOB CHARACTER SET ANY_CS);
  PRAGMA INTERFACE(c, dbms_lob_close);
  
  FUNCTION GETLENGTH (
      lob_loc              IN BLOB
  )
  RETURN NUMBER;
  PRAGMA INTERFACE(c, dbms_lob_getlength);
  
  FUNCTION GETLENGTH (
      lob_loc              IN CLOB CHARACTER SET ANY_CS
  )
  RETURN NUMBER;
  PRAGMA INTERFACE(c, dbms_lob_getlength);
  
  FUNCTION SUBSTR (
     lob_loc     IN    BLOB,
     amount      IN    INTEGER := 32767,
     offset      IN    INTEGER := 1)
  RETURN RAW;
  PRAGMA INTERFACE(c, dbms_lob_substr);

  FUNCTION SUBSTR (
     lob_loc     IN    CLOB CHARACTER SET ANY_CS,
     amount      IN    INTEGER := 32767,
     offset      IN    INTEGER := 1)
  RETURN VARCHAR2;
  PRAGMA INTERFACE(c, dbms_lob_substr);
  
  FUNCTION INSTR (
     lob_loc    IN   BLOB,
     pattern    IN   RAW,
     offset     IN   INTEGER := 1,
     nth        IN   INTEGER := 1)
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_lob_instr);

  FUNCTION INSTR (
     lob_loc    IN   CLOB CHARACTER SET ANY_CS,
     pattern    IN   VARCHAR2 CHARACTER SET ANY_CS,
     offset     IN   INTEGER := 1,
     nth        IN   INTEGER := 1)
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_lob_instr);

  FUNCTION COMPARE (
     lob_1    IN BLOB,
     lob_2    IN BLOB,
     amount   IN INTEGER := 50331648,
     offset_1 IN INTEGER := 1,
     offset_2 IN INTEGER := 1)
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_lob_compare);

  FUNCTION COMPARE (
       lob_1    IN CLOB CHARACTER SET ANY_CS,
       lob_2    IN CLOB CHARACTER SET ANY_CS,
       amount   IN INTEGER := 50331648,
       offset_1 IN INTEGER := 1,
       offset_2 IN INTEGER := 1)
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_lob_compare);

  PROCEDURE CREATETEMPORARY (
     lob_loc IN OUT NOCOPY BLOB,
     cache   IN            BOOLEAN,
     dur     IN            PLS_INTEGER := 10);
  PRAGMA INTERFACE(c, dbms_lob_createtemporary);
  
  PROCEDURE CREATETEMPORARY (
     lob_loc IN OUT NOCOPY CLOB CHARACTER SET ANY_CS,
     cache   IN            BOOLEAN,
     dur     IN            PLS_INTEGER := 10);
  PRAGMA INTERFACE(c, dbms_lob_createtemporary);

  PROCEDURE FREETEMPORARY (
     lob_loc  IN OUT  NOCOPY BLOB);
  PRAGMA INTERFACE(c, dbms_lob_freetemporary);

  PROCEDURE FREETEMPORARY (
     lob_loc  IN OUT  NOCOPY CLOB CHARACTER SET ANY_CS);
  PRAGMA INTERFACE(c, dbms_lob_freetemporary);

  PROCEDURE COPY (
    dest_lob    IN OUT NOCOPY BLOB,
    src_lob     IN            BLOB,
    amount      IN            INTEGER,
    dest_offset IN            INTEGER := 1,
    src_offset  IN            INTEGER := 1);
  PRAGMA INTERFACE(c, dbms_lob_copy);

  PROCEDURE COPY (
    dest_lob    IN OUT NOCOPY CLOB CHARACTER SET ANY_CS,
    src_lob     IN            CLOB CHARACTER SET ANY_CS,
    amount      IN            INTEGER,
    dest_offset IN            INTEGER := 1,
    src_offset  IN            INTEGER := 1);
  PRAGMA INTERFACE(c, dbms_lob_copy);

  PROCEDURE TRIM (
     lob_loc        IN OUT  NOCOPY BLOB,
     newlen         IN             INTEGER);
  PRAGMA INTERFACE(c, dbms_lob_trim);
  
  PROCEDURE TRIM (
       lob_loc        IN OUT  NOCOPY CLOB CHARACTER SET ANY_CS,
       newlen         IN             INTEGER);
  PRAGMA INTERFACE(c, dbms_lob_trim);
  
  
  PROCEDURE APPEND(dest_lob IN OUT NOCOPY clob CHARACTER SET ANY_CS, src_lob IN clob CHARACTER SET ANY_CS);
  PRAGMA INTERFACE(c, dbms_lob_append);

  PROCEDURE APPEND(dest_lob IN OUT NOCOPY blob, src_lob IN blob);
  PRAGMA INTERFACE(c, dbms_lob_append);


  PROCEDURE WRITE (
     lob_loc  IN OUT  NOCOPY BLOB,
     amount   IN             INTEGER,
     offset   IN             INTEGER,
     buffer   IN             RAW);
  PRAGMA INTERFACE(c, dbms_lob_write);

  PROCEDURE WRITE (
     lob_loc  IN OUT  NOCOPY CLOB CHARACTER SET ANY_CS,
     amount   IN             INTEGER,
     offset   IN             INTEGER,
     buffer   IN             VARCHAR2 CHARACTER SET ANY_CS);
  PRAGMA INTERFACE(c, dbms_lob_write);


  PROCEDURE OBCI_WRITE (
     lob_loc       IN OUT  NOCOPY CLOB CHARACTER SET ANY_CS,
     byte_amount   IN OUT         INTEGER,
     char_amount   IN OUT         INTEGER,
     offset        IN             INTEGER,
     buffer        IN             VARCHAR2 CHARACTER SET ANY_CS);
  PRAGMA INTERFACE(c, dbms_lob_obci_write);


  PROCEDURE WRITEAPPEND (
    lob_loc IN OUT NOCOPY BLOB,
    amount  IN            INTEGER,
    buffer  IN            RAW);
  PRAGMA INTERFACE(c, dbms_lob_writeappend);

  PROCEDURE WRITEAPPEND (
    lob_loc IN OUT NOCOPY CLOB CHARACTER SET ANY_CS,
    amount  IN            INTEGER,
    buffer  IN            VARCHAR2 CHARACTER SET ANY_CS);
  PRAGMA INTERFACE(c, dbms_lob_writeappend);


  PROCEDURE ERASE (
    lob_loc           IN OUT   NOCOPY   BLOB,
    amount            IN OUT   NOCOPY   INTEGER,
    offset            IN                INTEGER := 1);
  PRAGMA INTERFACE(c, dbms_lob_erase);

  PROCEDURE ERASE (
    lob_loc           IN OUT   NOCOPY   CLOB CHARACTER SET ANY_CS,
    amount            IN OUT   NOCOPY   INTEGER,
    offset            IN                INTEGER := 1);
  PRAGMA INTERFACE(c, dbms_lob_erase);


  PROCEDURE READ (
      lob_loc   IN             BLOB,
      amount    IN OUT NOCOPY  INTEGER,
      offset    IN             INTEGER,
      buffer    OUT            RAW);
  PRAGMA INTERFACE(c, dbms_lob_read);

  PROCEDURE READ (
      lob_loc   IN             CLOB CHARACTER SET ANY_CS,
      amount    IN OUT NOCOPY  INTEGER,
      offset    IN             INTEGER,
      buffer    OUT            VARCHAR2 CHARACTER SET ANY_CS);
  PRAGMA INTERFACE(c, dbms_lob_read);


  PROCEDURE CONVERTTOBLOB (
    dest_lob       IN OUT     NOCOPY BLOB,
    src_clob       IN         CLOB CHARACTER SET ANY_CS,
    amount         IN         INTEGER,
    dest_offset    IN OUT     INTEGER,
    src_offset     IN OUT     INTEGER,
    blob_csid      IN         NUMBER,
    lang_context   IN OUT     INTEGER,
    warning        OUT        INTEGER);
  PRAGMA INTERFACE(c, dbms_lob_converttoblob);
  
  FUNCTION ISTEMPORARY (
   lob_loc        IN BLOB
  )
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_lob_istemporary);
 
  FUNCTION ISTEMPORARY (
   lob_loc        IN CLOB CHARACTER SET ANY_CS
  )
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_lob_istemporary);

END DBMS_LOB;
//
