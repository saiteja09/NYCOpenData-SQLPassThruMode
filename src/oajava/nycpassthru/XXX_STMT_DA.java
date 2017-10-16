package oajava.nycpassthru;

/**
 * Created by sbobba on 9/11/2017.
 */
public class XXX_STMT_DA {
    long                dam_hstmt;      /* DAM handle to the statement */

    int                 iType;          /* Type of the query */
    int                 iFetchSize;
    int                 iRowCount;
    int                 iColCount;
    int                 iTableCount;
    int                 iCurTableNum;   /* used to identify if column belongs to current table */


    java.sql.ResultSet  rs;

    XXX_STMT_DA()
    {
        rs = null;
    }
}
