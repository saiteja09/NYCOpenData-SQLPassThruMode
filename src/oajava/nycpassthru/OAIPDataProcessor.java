package oajava.nycpassthru;

import oajava.sql.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.jar.Pack200;


import static oajava.sql.ip.*;

import static oajava.sql.jdam.*;

/**
 * Created by sbobba on 9/13/2017.
 */
public class OAIPDataProcessor {


    int process_rows(XXX_STMT_DA pStmtDA, ArrayList<String> dataFromAPI, String selectClause, TableInfo tableInfo){

        JSONArray rootJSON = new JSONArray(dataFromAPI.get(1));
        long htable = jdam.damex_getFirstTable(jdam.damex_getQuery(pStmtDA.dam_hstmt));
        while(htable != 0) {
            for (int i = 0; i < rootJSON.length(); i++) {
                JSONObject jsonRow = rootJSON.getJSONObject(i);

                long hrow = jdam.damex_allocRow(pStmtDA.dam_hstmt);


                boolean isOriginalSelectListComaptible = checkSelectListComaptibility(pStmtDA);

                xo_int piTableNum = new xo_int(0);
                StringBuffer wsTableName = new StringBuffer(DAM_MAX_ID_LEN + 1);
                jdam.damex_describeTable(htable, piTableNum, null, null, wsTableName, null, null);
                HashMap<String, Integer> columnInfo = tableInfo.getColumnsinTable(wsTableName.toString().toUpperCase());

                if (isOriginalSelectListComaptible) {
                    add_columns_select_original(pStmtDA, jsonRow, hrow, htable, columnInfo);
                } else {
                    add_columns_select_non_original(pStmtDA, jsonRow, hrow, htable, columnInfo, selectClause);
                }
                int retcode = jdam.damex_addRowToTable(pStmtDA.dam_hstmt, hrow);
                if (retcode != DAM_SUCCESS) {
                    //return retcode;
                    int hello = 1;
                }



            }
            htable = damex_getNextTable(jdam.damex_getQuery(pStmtDA.dam_hstmt));
        }
        return DAM_SUCCESS;
    }

    private void add_columns_select_non_original(XXX_STMT_DA pStmtDA, JSONObject jsonRow, long hrow, long htable, HashMap<String, Integer> columnInfo, String selectClause)
    {
        int dataType;
        String data;
        String[] selectList = selectClause.split(",");
        for(int i=0; i < selectList.length; i++)
        {
            String column = selectList[i];
            if(column.contains("(") && column.contains(")"))
            {
                column = column.replace('(', '_');
                column = column.replace(")","");
                data = getData(jsonRow, column.toUpperCase().toString());
                //Sample Response:
                /*[

                {
                    "COUNT_VEHICLE_YEAR": "2320790",
                        "VEHICLE_YEAR": "0"
                },
                {
                    "COUNT_VEHICLE_YEAR": "184",
                        "VEHICLE_YEAR": "1970"
                },
                {
                    "COUNT_VEHICLE_YEAR": "286",
                        "VEHICLE_YEAR": "1971"
                },
                {
                    "COUNT_VEHICLE_YEAR": "260",
                        "VEHICLE_YEAR": "1972"
                },*/

                int retCode = damex_addBigIntResValToRow(pStmtDA.dam_hstmt, hrow, i, Long.parseLong(data), (data != null) ? XO_NTS : ip.XO_NULL_DATA);
                System.out.print(retCode);
            }
            else
            {
                data = getData(jsonRow, column.toUpperCase().toString());
                dataType = columnInfo.get(column.toString().toLowerCase());
                int retCode = 0;
                switch(dataType){

                    case XO_TYPE_BIGINT:
                        retCode = damex_addBigIntResValToRow(pStmtDA.dam_hstmt, hrow, i, Long.parseLong(data), (data != null) ? ip.XO_NTS : ip.XO_NULL_DATA);
                        break;
                    case XO_TYPE_VARCHAR:
                        retCode = damex_addCharResValToRow(pStmtDA.dam_hstmt, hrow, i, data, (data != null) ? ip.XO_NTS : ip.XO_NULL_DATA);
                        break;

                    case XO_TYPE_TIMESTAMP_TYPE:

                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS");
                        Date parseDate = null;
                        try {
                            parseDate = dateFormat.parse(data);
                        }
                        catch (Exception ex)
                        {
                            System.out.println("Exception :" + ex.getMessage());
                        }

                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(parseDate);

                        xo_tm xo_timestamp = new xo_tm(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND), calendar.get(Calendar.MILLISECOND));

                        retCode = damex_addTimeStampResValToRow(pStmtDA.dam_hstmt, hrow, i, xo_timestamp, ip.XO_NTS);

                        break;
                }
            }
        }
    }

    private void add_columns_select_original(XXX_STMT_DA pStmtDA, JSONObject jsonRow, long hrow, long htable, HashMap<String, Integer> columnInfo) {
        int dataType;
        String data;
        long hcol = 0;
        hcol = damex_getFirstCol(htable, DAM_COL_IN_RESULT);

        while(hcol != 0)
        {
            StringBuffer colname = new StringBuffer();
            int iretcode = damex_describeCol(hcol, null, null, colname, null, null, null, null);
            dataType = columnInfo.get(colname.toString().toLowerCase());
            data = getData(jsonRow, colname.toString());
            int retCode = ip_process_column_val(data, dataType, hcol, hrow, pStmtDA);
            hcol = damex_getNextCol(htable);
        }
    }


    int ip_process_column_val(String data, int dataType, long hcol, long hrow, XXX_STMT_DA pStmtDA)
    {
        int retCode = 0;



        switch(dataType){

            case XO_TYPE_BIGINT:
                retCode = damex_addBigIntColValToRow(pStmtDA.dam_hstmt, hrow, hcol, Long.parseLong(data), (data != null) ? ip.XO_NTS : ip.XO_NULL_DATA);
                break;
            case XO_TYPE_VARCHAR:
                retCode = damex_addCharColValToRow(pStmtDA.dam_hstmt, hrow, hcol, data, (data != null) ? ip.XO_NTS : ip.XO_NULL_DATA);
                break;

            case XO_TYPE_TIMESTAMP_TYPE:

                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS");
                Date parseDate = null;
                try {
                    parseDate = dateFormat.parse(data);
                }
                catch (Exception ex)
                {
                    System.out.println("Exception :" + ex.getMessage());
                }

                Calendar calendar = Calendar.getInstance();
                calendar.setTime(parseDate);

                xo_tm xo_timestamp = new xo_tm(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND), calendar.get(Calendar.MILLISECOND));

                retCode = damex_addTimeStampColValToRow(pStmtDA.dam_hstmt, hrow, hcol, xo_timestamp, ip.XO_NTS);

                break;
        }

        return retCode;
    }


    String getData(JSONObject jsonRow, String column)
    {
        if(jsonRow.has(column)) {
            return jsonRow.getString(column);
        }
        else
        {
            return  null;
        }
    }

    boolean checkSelectListComaptibility(XXX_STMT_DA pStmt) {

        xo_int piSetQuantifier;
        xo_long phSelectValExpList, phGroupValExpList, phOrderValExpList;
        xo_long phSearchExp;
        xo_long phHavingExp;

        piSetQuantifier = new xo_int(0);
        phSelectValExpList = new xo_long(0);
        phGroupValExpList = new xo_long(0);
        phOrderValExpList = new xo_long(0);
        phSearchExp = new xo_long(0);
        phHavingExp = new xo_long(0);


        jdam.damex_describeSelectQuery(damex_getQuery(pStmt.dam_hstmt), piSetQuantifier,
                phSelectValExpList,
                phSearchExp,
                phGroupValExpList,
                phHavingExp,
                phOrderValExpList);

        SQLTreeParser sqlTreeParser = new SQLTreeParser(false, false, 0);
        boolean flag =sqlTreeParser.ip_isOriginalSelectListCompatible(phSelectValExpList.getVal(), phGroupValExpList.getVal(), phOrderValExpList.getVal());
        return flag;

    }

}
