#package_name:ODCICONST
#author: webber.wb

CREATE OR REPLACE NONEDITIONABLE PACKAGE ODCICONST IS

     pragma restrict_references(ODCIConst, WNDS, RNDS, WNPS, RNPS);

     Success          CONSTANT INTEGER  :=  0;
     Error            CONSTANT INTEGER  :=  1;
     Warning          CONSTANT INTEGER  :=  2;
     ErrContinue      CONSTANT INTEGER  :=  3;
     Fatal            CONSTANT INTEGER  :=  4;

     PredExactMatch   CONSTANT INTEGER  :=  1;
     PredPrefixMatch  CONSTANT INTEGER  :=  2;
     PredIncludeStart CONSTANT INTEGER  :=  4;
     PredIncludeStop  CONSTANT INTEGER  :=  8;
     PredObjectFunc   CONSTANT INTEGER  := 16;
     PredObjectPkg    CONSTANT INTEGER  := 32;
     PredObjectType   CONSTANT INTEGER  := 64;
     PredMultiTable   CONSTANT INTEGER  := 128;
     PredNotEqual     CONSTANT INTEGER  := 256;

     QueryFirstRows   CONSTANT INTEGER  :=  1;
     QueryAllRows     CONSTANT INTEGER  :=  2;
     QuerySortAsc     CONSTANT INTEGER  :=  4;
     QuerySortDesc    CONSTANT INTEGER  :=  8;
     QueryBlocking    CONSTANT INTEGER  := 16;

     CleanupCall      CONSTANT INTEGER  :=  1;
     RegularCall      CONSTANT INTEGER  :=  2;

     ObjectFunc       CONSTANT INTEGER  :=  1;
     ObjectPkg        CONSTANT INTEGER  :=  2;
     ObjectType       CONSTANT INTEGER  :=  4;

     ArgOther         CONSTANT INTEGER  :=  1;
     ArgCol           CONSTANT INTEGER  :=  2;
     ArgLit           CONSTANT INTEGER  :=  3;
     ArgAttr          CONSTANT INTEGER  :=  4;
     ArgNull          CONSTANT INTEGER  :=  5;
     ArgCursor        CONSTANT INTEGER  :=  6;

     PercentOption    CONSTANT INTEGER  :=  1;
     RowOption        CONSTANT INTEGER  :=  2;

     EstimateStats    CONSTANT INTEGER  :=  1;
     ComputeStats     CONSTANT INTEGER  :=  2;
     Validate         CONSTANT INTEGER  :=  4;

     AlterIndexNone           CONSTANT INTEGER  :=  0;
     AlterIndexRename         CONSTANT INTEGER  :=  1;
     AlterIndexRebuild        CONSTANT INTEGER  :=  2;
     AlterIndexRebuildOnline  CONSTANT INTEGER  :=  3;
     AlterIndexModifyCol      CONSTANT INTEGER  :=  4;
     AlterIndexUpdBlockRefs   CONSTANT INTEGER  :=  5;
     AlterIndexRenameCol      CONSTANT INTEGER  :=  6;
     AlterIndexRenameTab      CONSTANT INTEGER  :=  7;
     AlterIndexMigrate        CONSTANT INTEGER  :=  8;

     Local                    CONSTANT INTEGER  := 1;
     RangePartn               CONSTANT INTEGER  := 2;
     HashPartn                CONSTANT INTEGER  := 4;
     Online                   CONSTANT INTEGER  := 8;
     Parallel                 CONSTANT INTEGER  := 16;
     Unusable                 CONSTANT INTEGER  := 32;
     IndexOnIOT               CONSTANT INTEGER  := 64;
     TransTblspc              CONSTANT INTEGER  := 128;
     FunctionIdx              CONSTANT INTEGER  := 256;
     ListPartn                CONSTANT INTEGER  := 512;
     UpdateGlobalIndexes      CONSTANT INTEGER  := 1024;

     DefaultDegree            CONSTANT INTEGER  := 32767;

     DebuggingOn              CONSTANT INTEGER  :=  1;
     NoData                   CONSTANT INTEGER  :=  2;
     UserParamString          CONSTANT INTEGER  :=  4;
     RowMigration             CONSTANT INTEGER  :=  8;
     IndexKeyChanged          CONSTANT INTEGER  :=  16;

     None                     CONSTANT INTEGER  := 0;
     FirstCall                CONSTANT INTEGER  := 1;
     IntermediateCall         CONSTANT INTEGER  := 2;
     FinalCall                CONSTANT INTEGER  := 3;
     RebuildIndex             CONSTANT INTEGER  := 4;
     RebuildPMO               CONSTANT INTEGER  := 5;
     StatsGlobal              CONSTANT INTEGER  := 6;
     StatsGlobalAndPartition  CONSTANT INTEGER  := 7;
     StatsPartition           CONSTANT INTEGER  := 8;

     FetchOp                  CONSTANT INTEGER  := 1;
     PopulateOp               CONSTANT INTEGER  := 2;

     Sample                    CONSTANT INTEGER  := 1;
     SampleBlock               CONSTANT INTEGER  := 2;

     True                     CONSTANT INTEGER  := 1;
     False                    CONSTANT INTEGER  := 0;

     QueryCoordinator         CONSTANT INTEGER  := 1;
     Shadow                   CONSTANT INTEGER  := 2;
     Slave                    CONSTANT INTEGER  := 4;

     FetchEOS                 CONSTANT INTEGER  := 1;

     CompFilterByCol          CONSTANT INTEGER  := 1;
     CompOrderByCol           CONSTANT INTEGER  := 2;
     CompOrderDscCol          CONSTANT INTEGER  := 4;
     CompUpdatedCol           CONSTANT INTEGER  := 8;
     CompRenamedCol           CONSTANT INTEGER  := 16;
     CompRenamedTopADT        CONSTANT INTEGER  := 32;

     ColumnExpr               CONSTANT INTEGER  := 1;
     AncOpExpr                CONSTANT INTEGER  := 2;

     SortAsc                  CONSTANT INTEGER  := 1;
     SortDesc                 CONSTANT INTEGER  := 2;
     NullsFirst               CONSTANT INTEGER  := 4;

     AddPartition             CONSTANT INTEGER  := 1;
     DropPartition            CONSTANT INTEGER  := 2;

END ODCIConst;
//
