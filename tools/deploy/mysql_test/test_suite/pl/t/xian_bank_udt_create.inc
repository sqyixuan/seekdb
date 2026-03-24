#owner: linlin.xll
#owner group: sql1
#description: xian bank user define types
#tags: pl

--result_format 4

--disable_warnings

delimiter /;


CREATE OR REPLACE TYPE IDX_LOC_TYPE as table of VARCHAR2(13)
/

CREATE OR REPLACE TYPE LIMIT_DATA_OBJECT IS OBJECT(
       SEC_WAY VARCHAR2(2),
       SEC_WAY_NAME VARCHAR2(20),
       ONCE_LIMIT NUMBER(18,4),
       AVAIL_DAY_LIMIT NUMBER(18,4), --可用单日累计额度
       DAY_LIMIT NUMBER(18,4)--单日累计额度
  )
/

CREATE OR REPLACE TYPE LIMIT_DATA_OBJECT_NEW IS OBJECT(
       SEC_WAY VARCHAR2(2),
       SEC_WAY_NAME VARCHAR2(20),
       ONCE_LIMIT NUMBER(18,4),
       AVAIL_DAY_LIMIT NUMBER(18,4), --可用单日累计额度
       DAY_LIMIT NUMBER(18,4),--单日累计额度
       DAY_TOTAL INTEGER, --单日转账笔数
       AVAIL_DAY_TOTAL  INTEGER,--单日可用转账笔数                                                     ,
       YEAR_LIMIT NUMBER(18,4),--单年累计额度                                                                     ,
       AVAIL_YEAR_LIMIT NUMBER(18,4)  --单年可用累计额度
  )
/

CREATE OR REPLACE TYPE LIMIT_LIST_TYPE AS TABLE OF LIMIT_DATA_OBJECT
/

CREATE OR REPLACE TYPE LIMIT_LIST_TYPE_NEW AS TABLE OF LIMIT_DATA_OBJECT_NEW
/

#### TODO
## CREATE OR REPLACE TYPE OBJECTS_LIST IS TABLE OF VARCHAR2(32767)
## /
CREATE OR REPLACE TYPE OBJECTS_LIST IS TABLE OF VARCHAR2(4000);
/

CREATE OR REPLACE TYPE REPORT_TEMPLATE AS OBJECT (
  reportType INTEGER,
  useDescriptionInfo INTEGER,
  useQuantitativeInfo INTEGER,
  useDiagrams INTEGER,
  useTableColumns INTEGER,
  useTableColumnsComments INTEGER,
  useTableIndexes INTEGER,
  useTableConstraints INTEGER,
  useTableFKReferringTo INTEGER,
  useTableFKReferredFrom INTEGER,
  useEntityAttributes INTEGER,
  useEntityAttributesComments INTEGER,
  useEntityConstraints INTEGER,
  useEntityIdentifiers INTEGER,
  useEntityRelationships INTEGER,
  useEntityIncomingProcesses INTEGER,
  useEntityOutgoingProcesses  INTEGER,
  useDomainConstraints INTEGER,
  useDomainUsedInTables INTEGER,
  useDomainUsedInEntities INTEGER,
  useSTAttributes INTEGER,
  useSTAttributesComments INTEGER,
  useSTMethods INTEGER,
  useSTUsedInTables INTEGER,
  useSTUsedInEntities INTEGER,
  useCTUsedInTables INTEGER,
  useCTUsedInEntities INTEGER,
  useDTUsedInTables INTEGER,
  useDTUsedInEntities INTEGER,
  useCRImpactedObjects INTEGER,
  useMRImpactedObjects INTEGER)
/

delimiter ;/
