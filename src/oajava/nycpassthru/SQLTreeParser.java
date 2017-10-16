package oajava.nycpassthru;

import oajava.sql.*;

import java.math.BigInteger;
import java.util.HashMap;

import static oajava.sql.ip.*;

/**
 * Created by sbobba on 9/11/2017.
 */
public class SQLTreeParser {


    private boolean         m_bPushPostProcessing; /* Indicates if Post-processing (GROUP BY, ORDER BY etc)
                                                    should be sent to back-end or allow DAM to process */
    private boolean         m_bUseOriginalSelectList; /* Indicates if query sent to backend should use original select list
                                                        expressions or just columns in use. When original query
                                                        has GROUP BY, SET functions and  gbPushPostProcessing is FALSE,
                                                        IP should return values for base columns so that DAM can do the post-processing
                                                      */
    private long m_tmHandle;
    private StringBuffer whereCond;
    private StringBuffer orderByCond;
    private StringBuffer groupByCond;
    private StringBuffer havingCond;
    private StringBuffer selectClause;
    private StringBuffer topClause;


    public SQLTreeParser(boolean m_bPushPostProcessing, boolean m_bUseOriginalSelectList, long m_tmhandle)
    {
        this.m_bPushPostProcessing = m_bPushPostProcessing;
        this.m_bUseOriginalSelectList = m_bUseOriginalSelectList;
        this.m_tmHandle = m_tmhandle;
        whereCond = new StringBuffer();
        orderByCond = new StringBuffer();
        groupByCond = new StringBuffer();
        havingCond = new StringBuffer();
        selectClause = new StringBuffer();
        topClause = new StringBuffer();

    }


    /************************************************************************
     Function:       ip_format_query()
     Description:
     Return:
     ************************************************************************/
    int             ip_format_query(long hquery, StringBuffer pSqlBuffer)
    {
        int         iQueryType;

        iQueryType = jdam.damex_getQueryType(hquery);

        switch (iQueryType) {
            case DAM_SELECT:
                ip_format_select_query(hquery, pSqlBuffer);
                break;
            case DAM_INSERT:
/*            ip_format_insert_query(hquery, pSqlBuffer); */
                break;
            case DAM_UPDATE:
/*            ip_format_update_query(hquery, pSqlBuffer); */
                break;
            case DAM_DELETE:
/*            ip_format_delete_query(hquery, pSqlBuffer); */
                break;
            case DAM_TABLE:
                ip_format_table_query(hquery,pSqlBuffer);
            default:
                break;
        }

        return IP_SUCCESS;
    }

    /********************************************************************************************
     Method:         ip_format_select_query
     Description:    Format the given query
     Return:         IP_SUCCESS on success
     IP_FAILURE on error
     *********************************************************************************************/

    int ip_format_select_query(long hquery, StringBuffer pSqlBuffer)
    {
        xo_int            piSetQuantifier;
        xo_long           phSelectValExpList, phGroupValExpList, phOrderValExpList;
        xo_long           phSearchExp;
        xo_long           phHavingExp;
        xo_long            piTopResRows;
        xo_int            pbTopPercent;
        xo_int            piUnionType = new xo_int(0);
        xo_long           phUnionQuery = new xo_long(0);

        piSetQuantifier     = new xo_int(0);
        phSelectValExpList  = new xo_long(0);
        phGroupValExpList   = new xo_long(0);
        phOrderValExpList   = new xo_long(0);
        phSearchExp         = new xo_long(0);
        phHavingExp         = new xo_long(0);
        piTopResRows        = new xo_long(0);
        pbTopPercent        = new xo_int(0);

        try
        {
            jdam.damex_describeSelectQuery(hquery, piSetQuantifier,
                    phSelectValExpList,
                    phSearchExp,
                    phGroupValExpList,
                    phHavingExp,
                    phOrderValExpList);

            jdam.damex_describeSelectTopClause(hquery, piTopResRows, pbTopPercent);


    /* check if query cannot use orginal select expression */
            if (!m_bPushPostProcessing) {
                m_bUseOriginalSelectList = ip_isOriginalSelectListCompatible(phSelectValExpList.getVal(),
                        phGroupValExpList.getVal(), phOrderValExpList.getVal());
            }

    /* get the table list */
            {
                long        htable;

                htable = jdam.damex_getFirstTable(hquery);
                while (htable != 0) {
                    xo_int      piTableNum = new xo_int(0);
                    StringBuffer wsTableName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
                    long    hCol;
                    StringBuffer wsColName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);

                    jdam.damex_describeTable(htable, piTableNum, null, null, wsTableName, null, null);
        /*
        System.out.print("Table:" + sTableName +". Columns Used: ", sTableName);
        */
                    hCol = jdam.damex_getFirstCol(htable, DAM_COL_IN_USE);
                    while (hCol != 0) {
                        jdam.damex_describeCol(hCol, null, null, wsColName, null, null, null, null);
            /*
            System.out.print(sColName + ",");
            */
                        hCol = jdam.damex_getNextCol(htable);
                    }
        /*
        System.out.println("");
        */
                    htable = jdam.damex_getNextTable(hquery);
                }
            }

            pSqlBuffer.append("SELECT ");
            if(!m_bUseOriginalSelectList) {
                ip_format_col_in_use(hquery, pSqlBuffer);
            }
            else {
                if ((piSetQuantifier.getVal() == SQL_SELECT_DISTINCT) && (m_bPushPostProcessing))
                    pSqlBuffer.append("DISTINCT ");
                if (piTopResRows.getVal() != DAM_NOT_SET) {
                    pSqlBuffer.append("TOP ").append(piTopResRows.getVal()).append(" ");
                    topClause.append(piTopResRows.getVal());
                    if (pbTopPercent.getVal() != 0)
                        pSqlBuffer.append("PERCENT ");
                }
                ip_format_valexp_list(hquery, phSelectValExpList.getVal(), pSqlBuffer);

                int indexSelect = pSqlBuffer.indexOf("SELECT ");
                String[] columnsList = pSqlBuffer.substring(indexSelect + 7).split(",");
                int colCount = 0;
                for(String col: columnsList)
                {
                    if(colCount > 0)
                    {
                        selectClause.append(",");
                    }
                    int indexAS = col.indexOf("AS ");
                    String tempCol = col.substring(indexAS + 3);
                    tempCol = tempCol.replace("\"","");
                    selectClause.append(tempCol.trim());
                    colCount++;
                }
            }

            pSqlBuffer.append(" FROM ");
            ip_format_table_list(hquery, pSqlBuffer);
            if (phSearchExp.getVal() != 0) {
                pSqlBuffer.append("WHERE ");
                ip_format_logexp(hquery, phSearchExp.getVal(), pSqlBuffer);

                //Cleaning up WHERE condition for REST API
                int indexOfWhere = pSqlBuffer.indexOf("WHERE ");
                String rawWhere = pSqlBuffer.substring(indexOfWhere + 6);
                rawWhere = rawWhere.replace("\"", "");
                rawWhere = rawWhere.replaceAll("T0_Q" + hquery + ".", "");

                whereCond.append(rawWhere);

            }
            if ((phGroupValExpList.getVal() != 0) && (m_bPushPostProcessing)) {
                pSqlBuffer.append(" GROUP BY ");
                ip_format_group_list(hquery, phGroupValExpList.getVal(), pSqlBuffer);

                //cleaning up GROUP BY for REST API
                int indexOfGroup = pSqlBuffer.indexOf("GROUP BY");
                String rawGroup = pSqlBuffer.substring(indexOfGroup + 9);
                rawGroup = rawGroup.replace("\"","");
                rawGroup = rawGroup.replaceAll("T0_Q" + hquery + ".", "");

                groupByCond.append(rawGroup.trim());
            }
            if ((phHavingExp.getVal() != 0) && (m_bPushPostProcessing)) {
                pSqlBuffer.append(" HAVING ");
                ip_format_logexp(hquery, phHavingExp.getVal(), pSqlBuffer);

                //cleaning up HAVING for REST API
                int indexHaving = pSqlBuffer.indexOf(" HAVING ");
                String rawHaving = pSqlBuffer.substring(indexHaving + 8);
                rawHaving = rawHaving.replace("\"","");
                rawHaving = rawHaving.replaceAll("T0_Q" + hquery + ".", "");

                havingCond.append(rawHaving.trim());


            }

	/* check if query has a UNION clause */
            jdam.damex_describeUnionQuery(hquery, piUnionType, phUnionQuery);

            if (phUnionQuery.getVal() != 0) {
                pSqlBuffer.append(" UNION ");
                if (piUnionType.getVal() != 0)
                    pSqlBuffer.append(" ALL ");
                ip_format_select_query(phUnionQuery.getVal(), pSqlBuffer);
            }

            if ((phOrderValExpList.getVal() != 0) && (m_bPushPostProcessing)) {
                pSqlBuffer.append(" ORDER BY ");
                ip_format_order_list(hquery, phOrderValExpList.getVal(), pSqlBuffer);

                //clean up the order by for REST API
                int indexOfOrder = pSqlBuffer.indexOf("ORDER BY");
                String rawOrderBy = pSqlBuffer.substring(indexOfOrder + 9);
                rawOrderBy = rawOrderBy.replace("\"", "");
                rawOrderBy = rawOrderBy.replaceAll("T0_Q" + hquery + ".", "");

                orderByCond.append(rawOrderBy.trim());

            }

        }
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Error: " + e + "\n");
        }

        return IP_SUCCESS;

    }

    int     ip_format_col_in_use(long hquery, StringBuffer pSqlBuffer)
    {
        long       htable;
        int        iFirst = TRUE;

        htable = jdam.damex_getFirstTable(hquery);
        while (htable != 0) {
            xo_int      piTableNum = new xo_int(0);
            StringBuffer wsTableName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
            long    hCol;

            jdam.damex_describeTable(htable, piTableNum, null, null, wsTableName, null, null);
            hCol = jdam.damex_getFirstCol(htable, DAM_COL_IN_USE);

            while (hCol != 0) {
                if(iFirst == FALSE)
                    pSqlBuffer.append(", ");
                ip_format_col(hquery, hCol, pSqlBuffer);
                hCol = jdam.damex_getNextCol(htable);
                iFirst = FALSE;
            }
            htable = jdam.damex_getNextTable(hquery);
        }
        return IP_SUCCESS;
    }

    int     ip_format_col(long hquery, long hCol, StringBuffer pSqlBuffer)
    {
        xo_int         piTableNum, piColNum;
        StringBuffer wsColName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
        long         hParentQuery;

        piTableNum  = new xo_int(0);
        piColNum    = new xo_int(0);

        jdam.damex_describeCol(hCol,
                piTableNum,
                piColNum,
                wsColName, null, null, null, null);

        /* check if TableNum is valid. For COUNT(*) the column has iTableNum not set */
        if (piTableNum.getVal() == DAM_NOT_SET)
            return IP_SUCCESS;

        if(jdam.damex_getQueryType(hquery) == DAM_SELECT){
            if (jdam.damex_isCorrelatedCol(hCol) != 0) {
                hParentQuery = jdam.damex_getParentQuery(hquery);
                pSqlBuffer.append("T").append(piTableNum.getVal()).append("_Q").append(hParentQuery).append(".\"").append(wsColName).append("\"");
            }
            else
                pSqlBuffer.append("T").append(piTableNum.getVal()).append("_Q").append(hquery).append(".\"").append(wsColName).append("\"");;
        }
        else {
            pSqlBuffer.append("\"").append(wsColName).append("\"");
        }

        return IP_SUCCESS;
    }


    int     ip_format_valexp_list(long hquery, long hValExpList, StringBuffer pSqlBuffer)
    {
        long    hValExp;
        int     iFirst = TRUE;
        StringBuffer      wsAsColName = new StringBuffer(DAM_MAX_ID_LEN+1);

        hValExp = jdam.damex_getFirstValExp(hValExpList);
        while (hValExp != 0) {
            if (iFirst == FALSE)
                pSqlBuffer.append(", ");
            else
                iFirst = FALSE;

            ip_format_valexp(hquery, hValExp, pSqlBuffer);

            jdam.damex_describeValExpEx(hValExp, wsAsColName, null);
            if (wsAsColName.length() > 0)
                pSqlBuffer.append(" AS \"").append(wsAsColName).append("\" ");

            hValExp = jdam.damex_getNextValExp(hValExpList);
        }

        pSqlBuffer.append(" ");
        return IP_SUCCESS;
    }

    int     ip_format_valexp(long hquery, long hValExp, StringBuffer pSqlBuffer)
    {
        xo_int      piType = new xo_int(0); /* literal value, column, +, -, *, / etc   */
        xo_int      piFuncType = new xo_int(0);
        xo_long	    hLeftValExp = new xo_long(0);
        xo_long     hRightValExp = new xo_long(0);
        xo_long     hVal = new xo_long(0);
        xo_long     hScalarValExp = new xo_long(0);
        xo_long     hCaseValExp = new xo_long(0);
        int         iFuncType;
        xo_int	    piSign = new xo_int(0);

        jdam.damex_describeValExp(hValExp, piType, /* literal value, column, +, -, *, / etc   */
                piFuncType,
                hLeftValExp,
                hRightValExp,
                hVal,
                hScalarValExp,
                hCaseValExp
        );

        iFuncType = piFuncType.getVal();

        jdam.damex_describeValExpEx(hValExp, null, piSign);

        if(piSign.getVal() != 0){
            pSqlBuffer.append("-(");
        }

        /* function type */
        if ((iFuncType & SQL_F_COUNT_ALL) != 0) pSqlBuffer.append("COUNT(*) ");
        if ((iFuncType & SQL_F_COUNT) != 0) pSqlBuffer.append("COUNT ");
        if ((iFuncType & SQL_F_AVG) != 0) pSqlBuffer.append("AVG ");
        if ((iFuncType & SQL_F_MAX) != 0) pSqlBuffer.append("MAX ");
        if ((iFuncType & SQL_F_MIN) != 0) pSqlBuffer.append("MIN ");
        if ((iFuncType & SQL_F_SUM) != 0) pSqlBuffer.append("SUM ");

        if ((iFuncType != 0) && (iFuncType != SQL_F_COUNT_ALL))
            pSqlBuffer.append("( ");
        if ((iFuncType & SQL_F_DISTINCT) != 0) pSqlBuffer.append("DISTINCT ");
        switch (piType.getVal()) {
            case SQL_VAL_EXP_VAL:
                ip_format_val(hquery, hVal.getVal(), pSqlBuffer);
                break;
            case SQL_VAL_EXP_ADD:
                pSqlBuffer.append("( ");
                ip_format_valexp(hquery, hLeftValExp.getVal(), pSqlBuffer);
                pSqlBuffer.append(" + ");
                ip_format_valexp(hquery, hRightValExp.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");

                break;
            case SQL_VAL_EXP_SUBTRACT:
                pSqlBuffer.append("( ");
                ip_format_valexp(hquery, hLeftValExp.getVal(), pSqlBuffer);
                pSqlBuffer.append(" - ");
                ip_format_valexp(hquery, hRightValExp.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");

                break;

            case SQL_VAL_EXP_MULTIPLY:

                pSqlBuffer.append("( ");
                ip_format_valexp(hquery, hLeftValExp.getVal(), pSqlBuffer);
                pSqlBuffer.append(" * ");
                ip_format_valexp(hquery, hRightValExp.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");

                break;

            case SQL_VAL_EXP_DIVIDE:

                pSqlBuffer.append("( ");
                ip_format_valexp(hquery, hLeftValExp.getVal(), pSqlBuffer);
                pSqlBuffer.append(" / ");
                ip_format_valexp(hquery, hRightValExp.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");

                break;

            case SQL_VAL_EXP_SCALAR:
                ip_format_scalar_valexp(hquery, hScalarValExp.getVal(), pSqlBuffer);
                break;

            case SQL_VAL_EXP_CASE:
                ip_format_case_valexp(hquery, hCaseValExp.getVal(), pSqlBuffer);
                break;

            default:            pSqlBuffer.append("Invalid Value Expression Type:").append(piType.getVal());
                break;
        }

        if ((iFuncType != 0) && (iFuncType != SQL_F_COUNT_ALL))
            pSqlBuffer.append(")");


        if (piSign.getVal() != 0) {
            pSqlBuffer.append(")");
        }
        return IP_SUCCESS;
    }

    int     ip_format_scalar_cast(long hquery, long hValExpList, StringBuffer pSqlBuffer)
    {
        StringBuffer    sName;
        long            hValExp;
        xo_int         iResXoType = new xo_int(0);
        xo_int         iLength = new xo_int(0);
        xo_int         iPrecision = new xo_int(0);
        xo_int         iScale = new xo_int(0);
        String         sTypeName;

        sName = new StringBuffer(ip.DAM_MAX_ID_LEN + 1);
        hValExp = jdam.damex_getFirstValExp(hValExpList);
        ip_format_valexp(hquery, hValExp, pSqlBuffer);

        jdam.dam_describeScalarEx(hValExpList, sName, null, iResXoType, iLength, iPrecision, iScale);
        sTypeName = xxx_map_type_to_name(iResXoType.getVal());
        pSqlBuffer.append(" AS " + sTypeName);
        if (iResXoType.getVal() == ip.XO_TYPE_NUMERIC) {
            pSqlBuffer.append("(" + iPrecision.getVal() + "," + iScale.getVal() + ")");
        }

        return IP_SUCCESS;
    }

    int     ip_format_scalar_valexp(long hquery, long   hScalarValExp, StringBuffer pSqlBuffer)
    {
        StringBuffer    sName;
        xo_long      phValExpList;

        phValExpList = new xo_long();
        sName = new StringBuffer(ip.DAM_MAX_ID_LEN + 1);

        jdam.damex_describeScalarValExp(hScalarValExp, sName, phValExpList);

        /* check if scalar function refers to a special @@ identifier */
        if((sName.substring(0,2)) .equals ("@@")) {
            pSqlBuffer.append(sName);
            return IP_SUCCESS;
        }

            /* handle CONVERT function */
        if (sName.toString().equals("CONVERT")) {
            pSqlBuffer.append("{fn ");
            pSqlBuffer.append(sName);
            pSqlBuffer.append("( ");
            //if (phValExpList.getVal() != 0) ip_format_scalar_convert(hquery, phValExpList.getVal(), pSqlBuffer);
            pSqlBuffer.append(") }" );
        }
        else if (sName.toString().equals("CAST")) {
            pSqlBuffer.append(sName);
            pSqlBuffer.append("( ");
            if (phValExpList.getVal() != 0) ip_format_scalar_cast(hquery, phValExpList.getVal(), pSqlBuffer);
            pSqlBuffer.append(")" );
        }
        else {
            pSqlBuffer.append(sName);
            pSqlBuffer.append("( ");
            if (phValExpList.getVal() != 0)
                ip_format_valexp_list(hquery, phValExpList.getVal(), pSqlBuffer);
            pSqlBuffer.append(") ");
        }
        return IP_SUCCESS;
    }

    int     ip_format_case_valexp(long hquery, long   hCaseValExp, StringBuffer pSqlBuffer)
    {
        xo_long             hInputValExp, hCaseElemList, hElseValExp;

        hInputValExp = new xo_long(0);
        hCaseElemList = new xo_long(0);
        hElseValExp = new xo_long(0);
        jdam.damex_describeCaseValExp(hCaseValExp, hInputValExp, hCaseElemList, hElseValExp);
        pSqlBuffer.append("CASE ");
        if (hInputValExp.getVal() != 0)
            ip_format_valexp(hquery, hInputValExp.getVal(), pSqlBuffer);
        pSqlBuffer.append(" ");
        ip_format_case_elem_list(hquery, hCaseElemList.getVal(), pSqlBuffer);
        if (hElseValExp.getVal() != 0) {
            pSqlBuffer.append(" ELSE ");
            ip_format_valexp(hquery, hElseValExp.getVal(), pSqlBuffer);
        }
        pSqlBuffer.append(" END ");

        return IP_SUCCESS;
    }

    int     ip_format_case_elem_list(long hquery, long   hCaseElemList, StringBuffer pSqlBuffer)
    {
        long             hCaseElem;
        xo_long          hWhenValExp;
        xo_long          hWhenBoolExp;
        xo_long          hResValExp;

        hWhenValExp = new xo_long(0);
        hWhenBoolExp = new xo_long(0);
        hResValExp = new xo_long(0);
        hCaseElem = jdam.damex_getFirstCaseElem(hCaseElemList);
        while (hCaseElem != 0) {
            pSqlBuffer.append(" WHEN ");

            jdam.damex_describeCaseElem(hCaseElem, hWhenValExp, hWhenBoolExp, hResValExp);
            if (hWhenValExp.getVal() != 0) ip_format_valexp(hquery, hWhenValExp.getVal(), pSqlBuffer);
            if (hWhenBoolExp.getVal() != 0) ip_format_logexp(hquery, hWhenBoolExp.getVal(), pSqlBuffer);
            pSqlBuffer.append(" THEN ");
            ip_format_valexp(hquery, hResValExp.getVal(), pSqlBuffer);

            hCaseElem = jdam.damex_getNextCaseElem(hCaseElemList);
        }

        return IP_SUCCESS;
    }

    int     ip_format_val(long   hquery, long hVal, StringBuffer pSqlBuffer)
    {
        int         iType; /* literal value, column */
        int         iXoType; /* type of literal value - INTEGER, CHAR etc */
        xo_int      piType, piXoType, piValLen;
        xo_long     hCol;
        xo_long     hSubQuery;
        xo_int      piValStatus;
        Object      pData;

        piType = new xo_int();
        piXoType = new xo_int();
        piValLen =  new xo_int(0);
        hCol = new xo_long();
        hSubQuery = new xo_long();
        piValStatus = new xo_int();


        pData = jdam.damex_describeVal(hVal, piType,
                piXoType,
                piValLen,
                hCol,
                hSubQuery, piValStatus);

        iType = piType.getVal();
        iXoType = piXoType.getVal();

        switch (iType) {

            case SQL_VAL_DATA_CHAIN:
                pSqlBuffer.append("?");
    /*                ghValBlob = hVal; */
                break;
            case SQL_VAL_NULL:
                pSqlBuffer.append("NULL"); break;
            case SQL_VAL_QUERY: /* query */
                pSqlBuffer.append("( ");
                ip_format_query(hSubQuery.getVal(),pSqlBuffer);
                pSqlBuffer.append(" )");
                break;
            case SQL_VAL_COL: /* value is the column value */
                ip_format_col(hquery, hCol.getVal(), pSqlBuffer); break;
            case SQL_VAL_INTERVAL:
                break;
            case SQL_VAL_LITERAL: /* value is a Xo Type literal */
            {
                String  strObject;
                Integer iObject;
                xo_tm   xoTime;
                Double  dObject;
                Float   fObject;
                Short   sObject;
                Boolean bObject;
                Byte    byObject;
                BigInteger bigIntObject;

                switch (iXoType) {
                    case XO_TYPE_CHAR: /* pVal is a char literal */
                    case XO_TYPE_VARCHAR:
                    case XO_TYPE_NUMERIC:
                    case XO_TYPE_DECIMAL:
                        strObject = (String) pData;
                        ip_format_string_literal(strObject,pSqlBuffer);
                        break;
                    case XO_TYPE_WCHAR: /* pVal is a wchar literal */
                    case XO_TYPE_WVARCHAR:
                        strObject = (String) pData;
                        pSqlBuffer.append("N'").append(strObject).append("'");
                        break;
                    case XO_TYPE_INTEGER:  /* pVal is a integer literal */
                        iObject = (Integer) pData;
                        pSqlBuffer.append(iObject.intValue());
                        break;
                    case XO_TYPE_SMALLINT: /* pVal is small integer literal */
                        sObject = (Short) pData;
                        pSqlBuffer.append(sObject.shortValue());
                        break;
                    case XO_TYPE_FLOAT: /* pVal is a double literal */
                    case XO_TYPE_DOUBLE:
                        dObject = (Double) pData;
                        pSqlBuffer.append(dObject.doubleValue());
                        break;
                    case XO_TYPE_REAL: /* pVal is a float literal */
                        fObject = (Float) pData;
                        pSqlBuffer.append(fObject.floatValue());
                        break;
                    case XO_TYPE_DATE:
                        xoTime = (xo_tm)pData;
                        pSqlBuffer.append("{d '").append(xoTime.getVal(xo_tm.YEAR)).append("-").append(xoTime.getVal(xo_tm.MONTH)+1).append("-").append(xoTime.getVal(xo_tm.DAY_OF_MONTH)).append("'}");
                        break;
                    case XO_TYPE_TIME:
                        xoTime = (xo_tm)pData;
                        pSqlBuffer.append("{t '").append(" ").append(xoTime.getVal(xo_tm.HOUR)).append(":").append(xoTime.getVal(xo_tm.MINUTE)).append(":").append(xoTime.getVal(xo_tm.SECOND)).append("'}");
                        break;

                    case XO_TYPE_BIGINT:
                        bigIntObject = new BigInteger(pData.toString());
                        pSqlBuffer.append(bigIntObject);
                        break;
                    case XO_TYPE_TIMESTAMP:
                        xoTime = (xo_tm)pData;
                        if (xoTime.getVal(xo_tm.FRACTION) > 0) {
                            int     frac;

                            frac = (int) (xoTime.FRACTION * 0.000001);
                            pSqlBuffer.append("{ts '").append(xoTime.getVal(xo_tm.YEAR)).append("-").append(xoTime.getVal(xo_tm.MONTH)+1).append("-").append(xoTime.getVal(xo_tm.DAY_OF_MONTH))
                                    .append(" ").append(xoTime.getVal(xo_tm.HOUR)).append(":").append(xoTime.getVal(xo_tm.MINUTE)).append(":").append(xoTime.getVal(xo_tm.SECOND))
                                    .append(".").append(xoTime.getVal(xo_tm.FRACTION)).append("'}");
                        }
                        else {
                            pSqlBuffer.append("{ts '").append(xoTime.getVal(xo_tm.YEAR)).append("-").append(xoTime.getVal(xo_tm.MONTH)+1).append("-").append(xoTime.getVal(xo_tm.DAY_OF_MONTH))
                                    .append(" ").append(xoTime.getVal(xo_tm.HOUR)).append(":").append(xoTime.getVal(xo_tm.MINUTE)).append(":").append(xoTime.getVal(xo_tm.SECOND)).append("'}");
                        }

                        break;

                    case XO_TYPE_BIT:
                        bObject = (Boolean)pData;
                        pSqlBuffer.append(bObject.booleanValue()?1:0);
                        break;

                    case XO_TYPE_TINYINT:

                        byObject = (Byte)pData;
                        pSqlBuffer.append(byObject.byteValue());
                        break;

                    default:
                        pSqlBuffer.append("Invalid Xo Value Type:").append(iXoType);
                        break;
                }
            }
            break;
            default:
                pSqlBuffer.append("Invalid Value Type:").append(iType); break;
        }
        return IP_SUCCESS;

    }

    int     ip_format_table_list(long hquery, StringBuffer pSqlBuffer)
    {
        long              htable;
        int               iFirst = TRUE;
        xo_int		      piTableNum = new xo_int(0);
        StringBuffer      wsTableName = new StringBuffer(DAM_MAX_ID_LEN+1);
        int				  iJoinType;
        xo_int			  piJoinType = new xo_int(0);
        xo_long		      phJoinExp = new xo_long(0);
        long		hTableSubQuery;

        htable = jdam.damex_getFirstTable(hquery);
        while (htable != 0) {
            jdam.damex_describeTable(htable, piTableNum, null, null, wsTableName, null, null);

            /* check if table subquery */
            hTableSubQuery = jdam.damex_isTableSubQuery(htable);

            if(hTableSubQuery != 0){
                if (iFirst == FALSE) {
                    pSqlBuffer.append(", ");
                }
                pSqlBuffer.append("( ");
                ip_format_query(hTableSubQuery,pSqlBuffer);
                pSqlBuffer.append(" ) ");
                pSqlBuffer.append("T").append(piTableNum.getVal()).append("_Q").append(hquery);
                iFirst = FALSE;
                htable = jdam.damex_getNextTable(hquery);
                continue;
            }
            phJoinExp.setVal(0);

            jdam.damex_describeTableJoinInfo(htable, piJoinType, phJoinExp);

            iJoinType = piJoinType.getVal();

            switch (iJoinType) {
                case SQL_JOIN_LEFT_OUTER:
                    pSqlBuffer.append(" LEFT OUTER JOIN ");
                    break;
                case SQL_JOIN_RIGHT_OUTER:
                    pSqlBuffer.append(" RIGHT OUTER JOIN ");
                    break;
                case SQL_JOIN_FULL_OUTER:
                    pSqlBuffer.append(" FULL OUTER JOIN ");
                    break;
                case SQL_JOIN_INNER:
                    pSqlBuffer.append(" INNER JOIN ");
                    break;
                case SQL_JOIN_OLD_STYLE:
                    if (iFirst == FALSE)
                        pSqlBuffer.append(", ");
                    break;
            }
            pSqlBuffer.append(wsTableName).append(" T").append(piTableNum.getVal()).append("_Q").append(hquery);
            if (phJoinExp.getVal() != 0) {
                pSqlBuffer.append(" ON ");
                ip_format_logexp(hquery, phJoinExp.getVal(), pSqlBuffer);
            }

            iFirst = FALSE;
            htable = jdam.damex_getNextTable(hquery);
        }

        pSqlBuffer.append(" ");
        return IP_SUCCESS;
    }

    /********************************************************************************************
     Method:         ip_format_logexp
     Description:    Logical expression handling
     Return:         IP_SUCCESS on success
     IP_FAILURE on error
     *********************************************************************************************/
    int     ip_format_logexp(long hquery, long hLogExp, StringBuffer pSqlBuffer)
    {
        xo_int         iType; /* AND, OR , NOT or CONDITION */
        xo_long        hLeft, hRight;
        xo_long        hCond;

        iType = new xo_int(0);
        hLeft = new xo_long(0);
        hRight = new xo_long(0);
        hCond = new xo_long(0);

        jdam.damex_describeLogicExp(hLogExp,
                iType, /* AND, OR , NOT or CONDITION */
                hLeft,
                hRight,
                hCond);

        switch (iType.getVal()) {
            case SQL_EXP_COND:
                pSqlBuffer.append("( ");
                ip_format_cond(hquery, hCond.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");
                break;
            case SQL_EXP_AND:
                pSqlBuffer.append("( ");
                ip_format_logexp(hquery, hLeft.getVal(), pSqlBuffer);
                pSqlBuffer.append(" AND ");
                ip_format_logexp(hquery, hRight.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");

                break;
            case SQL_EXP_OR:
                pSqlBuffer.append("( ");
                ip_format_logexp(hquery, hLeft.getVal(), pSqlBuffer);
                pSqlBuffer.append(" OR ");
                ip_format_logexp(hquery, hRight.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");

                break;
            case SQL_EXP_NOT:
                pSqlBuffer.append("( ");
                pSqlBuffer.append(" NOT ");
                ip_format_logexp(hquery, hLeft.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");
                break;
            default:            pSqlBuffer.append("Invalid Expression Type:").append(iType);
                break;

        }

        return IP_SUCCESS;
    }

    /********************************************************************************************
     Method:         ip_format_cond
     Description:    Condition, Operator handling
     Return:         IP_SUCCESS on success
     IP_FAILURE on error
     *********************************************************************************************/
    int ip_format_cond(long hquery, long hCond, StringBuffer pSqlBuffer)
    {
        xo_int     piType;
        xo_long    hLeft, hRight, hExtra;
        int        iType;

        piType = new xo_int(0);
        hLeft = new xo_long(0);
        hRight = new xo_long(0);
        hExtra = new xo_long(0);

        jdam.damex_describeCond(hCond,
                piType, /* >, <, =, BETWEEN etc.*/
                hLeft,
                hRight,
                hExtra); /* used for BETWEEN */

        iType = piType.getVal();

         /* EXISTS and UNIQUE predicates */
        if ((iType & (SQL_OP_EXISTS | SQL_OP_UNIQUE)) != 0) {

            if ((iType & SQL_OP_NOT) != 0) pSqlBuffer.append(" NOT ");
            if ((iType & SQL_OP_EXISTS) != 0) pSqlBuffer.append(" EXISTS (");
            if ((iType & SQL_OP_UNIQUE) != 0) pSqlBuffer.append(" UNIQUE (");

            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
            pSqlBuffer.append(" )");
        }


        /* conditional predicates */
        if ((iType & ( SQL_OP_SMALLER | SQL_OP_GREATER |  SQL_OP_EQUAL)) != 0) {
            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);

            if ((iType & SQL_OP_NOT) != 0) {
                if ((iType & SQL_OP_EQUAL) != 0)
                    pSqlBuffer.append(" <> ");
            }
            else {
                pSqlBuffer.append(" ");
                if ((iType & SQL_OP_SMALLER) != 0) pSqlBuffer.append("<");
                if ((iType & SQL_OP_GREATER) != 0) pSqlBuffer.append(">");
                if ((iType & SQL_OP_EQUAL) != 0) pSqlBuffer.append("=");
                pSqlBuffer.append(" ");
            }

            if ((iType & (SQL_OP_QUANTIFIER_ALL | SQL_OP_QUANTIFIER_SOME | SQL_OP_QUANTIFIER_ANY)) != 0) {
                if ((iType & SQL_OP_QUANTIFIER_ALL) != 0)
                    pSqlBuffer.append(" ALL ( ");
                if ((iType & SQL_OP_QUANTIFIER_SOME) != 0)
                    pSqlBuffer.append(" SOME ( ");
                if ((iType & SQL_OP_QUANTIFIER_ANY) != 0)
                    pSqlBuffer.append(" ANY ( ");
            }

            ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);
        }
        /* like predicate */
        if ((iType & SQL_OP_LIKE) != 0) {
            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
            if ((iType & SQL_OP_NOT) != 0)
                pSqlBuffer.append(" NOT ");
            pSqlBuffer.append(" LIKE ");
            ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);

            if (hExtra.getVal() != 0) {
                pSqlBuffer.append(" ESCAPE ");
                ip_format_valexp(hquery, hExtra.getVal(), pSqlBuffer);
            }

        }

        /* Is NULL predicate */
        if ((iType & SQL_OP_ISNULL) != 0) {
            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
            if ((iType & SQL_OP_NOT) != 0)
                pSqlBuffer.append(" IS NOT NULL ");
            else
                pSqlBuffer.append(" IS NULL ");
        }

        /* IN predicate */
        if ((iType & SQL_OP_IN) != 0) {
            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
            if ((iType & SQL_OP_NOT) != 0)
                pSqlBuffer.append(" NOT ");
            pSqlBuffer.append(" IN ");
            ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);
        }

        /* BETWEEN predicate */
        if ((iType & SQL_OP_BETWEEN) != 0) {

            /* check if the between is a form of ( >= and < ) OR (> and <)
                OR (> and <=)
            */
            if (((iType & SQL_OP_BETWEEN_OPEN_LEFT) != 0) || ((iType & SQL_OP_BETWEEN_OPEN_RIGHT) != 0)) {
                /* format it as two conditions */
                ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
                if ((iType & SQL_OP_BETWEEN_OPEN_LEFT) != 0)
                    pSqlBuffer.append(" > ");
                else
                    pSqlBuffer.append(" >= ");
                ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);

                pSqlBuffer.append(" AND ");

                ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
                if ((iType & SQL_OP_BETWEEN_OPEN_RIGHT) != 0)
                    pSqlBuffer.append(" < ");
                else
                    pSqlBuffer.append(" <= ");
                ip_format_valexp(hquery, hExtra.getVal(), pSqlBuffer);
            }
            else {
                /* standard BETWEEN pattern */
                ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);

                if ((iType & SQL_OP_NOT) != 0)
                    pSqlBuffer.append(" NOT ");
                pSqlBuffer.append(" BETWEEN ");

                ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);
                pSqlBuffer.append(" AND ");
                ip_format_valexp(hquery, hExtra.getVal(), pSqlBuffer);
            }

        }

        return IP_SUCCESS;
    }

    int     ip_format_group_list(long hquery, long hValExpList, StringBuffer pSqlBuffer)
    {
        ip_format_valexp_list(hquery, hValExpList, pSqlBuffer);
        return IP_SUCCESS;
    }

    int     ip_format_order_list(long   hquery, long hValExpList, StringBuffer pSqlBuffer)
    {
        long            hValExp;
        xo_int          piResultColNum = new xo_int(0);
        xo_int          piSortOrder = new xo_int(0);
        int             iFirst = TRUE;

        hValExp = jdam.damex_getFirstValExp(hValExpList);
        while (hValExp != 0) {

            if (iFirst == FALSE)
                pSqlBuffer.append(", ");
            else
                iFirst = FALSE;

            jdam.damex_describeOrderByExp(hValExp, piResultColNum, piSortOrder);

            /*if (piResultColNum.getVal() != DAM_NOT_SET) /* use the result column number */
               /* pSqlBuffer.append(piResultColNum.getVal()+1);
            else*/
                ip_format_valexp(hquery, hValExp, pSqlBuffer);

            if (piSortOrder.getVal() == SQL_ORDER_ASC)
                pSqlBuffer.append(" ASC");
            else if (piSortOrder.getVal() == SQL_ORDER_DESC)
                pSqlBuffer.append(" DESC");

            hValExp = jdam.damex_getNextValExp(hValExpList);
        }

        pSqlBuffer.append(" ");
        return IP_SUCCESS;
    }

    int ip_format_string_literal(String pString,StringBuffer pSqlBuffer)
    {
        pSqlBuffer.append("'");
        String resultStr = pString.replaceAll("'","''");
        pSqlBuffer.append(resultStr);
        pSqlBuffer.append("'");
        return IP_SUCCESS;
    }

    int     ip_format_table_query(long hquery, StringBuffer pSqlBuffer)
    {
        long         hVal;
        int          iFirst = TRUE;

        hVal = jdam.damex_getFirstTableQueryVal(hquery);
        while (hVal != 0) {
            if (iFirst == FALSE)
                pSqlBuffer.append(", ");
            else
                iFirst = FALSE;

            ip_format_val(hquery, hVal, pSqlBuffer);
            hVal = jdam.damex_getNextTableQueryVal(hquery);
        }
        return IP_SUCCESS;
    }
    /************************************************************************
     Function:       ip_isOriginalSelectListCompatible()
     Description:
     Return:
     ************************************************************************/
    boolean             ip_isOriginalSelectListCompatible(long hSelectValExpList,
                                                          long hGroupValExpList,
                                                          long hOrderValExpList)
    {

            /* Check if there are Set functions in the Query */
        {
            long    hValExp;
            xo_int  piType = new xo_int(0);
            xo_int  piFuncType = new xo_int(0);
            xo_long	phLeftValExp = new xo_long(0);
            xo_long phRightValExp = new xo_long(0);
            xo_long phVal = new xo_long(0);
            xo_long phScalarValExp = new xo_long(0);
            xo_long phCaseValExp = new xo_long(0);

            hValExp = jdam.damex_getFirstValExp(hSelectValExpList);
            while (hValExp != 0) {
                jdam.damex_describeValExp(hValExp, piType, piFuncType, phLeftValExp, phRightValExp,phVal,phScalarValExp, phCaseValExp);

                if (piFuncType.getVal() != 0) return false;
                hValExp = jdam.damex_getNextValExp(hSelectValExpList);
            }
        }

            /* Check for GROUP BY */
        if (hGroupValExpList != 0) return false;

            /* check if ORDER BY does not refer to result columns */
        if (hOrderValExpList != 0) {
            long        hValExp;
            xo_int      piResultColNum = new xo_int(0);
            xo_int      piSortOrder = new xo_int(0);

            hValExp = jdam.damex_getFirstValExp(hOrderValExpList);
            while (hValExp != 0) {

                jdam.damex_describeOrderByExp(hValExp, piResultColNum, piSortOrder);
                if (piResultColNum.getVal() == DAM_NOT_SET) return false;

                hValExp = jdam.damex_getNextValExp(hOrderValExpList);
            }
        }

        return true;
    }

    public String  xxx_map_type_to_name(int iType)
    {
        switch (iType) {
            case ip.XO_TYPE_CHAR:return "CHAR";
            case ip.XO_TYPE_VARCHAR:return "VARCHAR";
            case ip.XO_TYPE_NUMERIC:return "NUMERIC";
            case ip.XO_TYPE_INTEGER:return "INTEGER";
            case ip.XO_TYPE_SMALLINT:return "SMALLINT";
            case ip.XO_TYPE_DOUBLE:return "DOUBLE";
        }
        return "ERROR";
    }


    HashMap<String, String> getConditions(){
        HashMap<String, String> conditions = new HashMap<>();
        conditions.put("where", whereCond.toString());
        conditions.put("select", selectClause.toString());
        conditions.put("top", topClause.toString());
        conditions.put("order", orderByCond.toString());
        conditions.put("group", groupByCond.toString());
        conditions.put("having", havingCond.toString());

        return conditions;
    }

}
