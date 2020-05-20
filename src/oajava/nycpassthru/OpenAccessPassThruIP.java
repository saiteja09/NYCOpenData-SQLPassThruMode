/* damip.java
 *
 * Copyright (c) 1995-2012 Progress Software Corporation. All Rights Reserved.
 *
 *
 * Description:     Template DAM IP
 *                  - is implemented in "JAVA"
 *                  - supports SELECT operations
 *					- Support Dynamic Schema
 */

package oajava.nycpassthru;

import oajava.sql.*;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/* define the class template to implement the sample IP */
public class OpenAccessPassThruIP implements oajava.sql.ip
{
    private long m_tmHandle = 0;
    final static int MAX_QUERY_LEN = 1024;
    private RESTHelper restHelper = null;
    private OA_IPHelper oa_ipHelper = null;
    private TableInfo tableInfo = null;
    private OAIPDataProcessor oaipDataProcessor = null;
    //private OADataHelper oaDataHelper = null;

    private String baseURL = null;
    private String resourceUniqueId = null;
    private XXX_STMT_DA[]   m_stmtDA;

    private boolean         m_bPushPostProcessing; /* Indicates if Post-processing (GROUP BY, ORDER BY etc)
                                                    should be sent to back-end or allow DAM to process */
    private boolean         m_bUseOriginalSelectList; /* Indicates if query sent to backend should use original select list
                                                        expressions or just columns in use. When original query
                                                        has GROUP BY, SET functions and  gbPushPostProcessing is FALSE,
                                                        IP should return values for base columns so that DAM can do the post-processing
                                                      */



    final static String OA_CATALOG_NAME   = "SCHEMA";        /* SCHEMA */
    final static String OA_USER_NAME      = "OAUSER";        /* OAUSER */

    /* Support array */
    private final int[]   ip_support_array =
                    {
                        0,
                        1, /* IP_SUPPORT_SELECT */
                        0, /* IP_SUPPORT_INSERT */
                        0, /* IP_SUPPORT_UPDATE */
                        0, /* IP_SUPPORT_DELETE */
                        1, /* IP_SUPPORT_SCHEMA - IP supports Schema Functions */
                        0, /* IP_SUPPORT_PRIVILEGES  */
                        0, /* IP_SUPPORT_OP_EQUAL */
                        0, /* IP_SUPPORT_OP_NOT   */
                        0, /* IP_SUPPORT_OP_GREATER */
                        0, /* IP_SUPPORT_OP_SMALLER */
                        0, /* IP_SUPPORT_OP_BETWEEN */
                        0, /* IP_SUPPORT_OP_LIKE    */
                        0, /* IP_SUPPORT_OP_NULL    */
                        0, /* IP_SUPPORT_SELECT_FOR_UPDATE */
                        0, /* IP_SUPPORT_START_QUERY */
                        0, /* IP_SUPPORT_END_QUERY */
                        0, /* IP_SUPPORT_UNION_CONDLIST */
                        0, /* IP_SUPPORT_CREATE_TABLE */
                        0, /* IP_SUPPORT_DROP_TABLE */
                        0, /* IP_SUPPORT_CREATE_INDEX */
                        0, /* IP_SUPPORT_DROP_INDEX */
                        0, /* IP_SUPPORT_PROCEDURE */
                        0, /* IP_SUPPORT_CREATE_VIEW */
                        0, /* IP_SUPPORT_DROP_VIEW */
                        0, /* IP_SUPPORT_QUERY_VIEW */
                        0, /* IP_SUPPORT_CREATE_USER */
                        0, /* IP_SUPPORT_DROP_USER */
                        0, /* IP_SUPPORT_CREATE_ROLE */
                        0, /* IP_SUPPORT_DROP_ROLE */
                        0, /* IP_SUPPORT_GRANT */
                        0, /* IP_SUPPORT_REVOKE */
                        1,  /* IP_SUPPORT_PASSTHROUGH_QUERY */
                        1,  /* IP_SUPPORT_NATIVE_COMMAND */
                        0,  /* IP_SUPPORT_ALTER_TABLE */
                        0,  /* IP_SUPPORT_BLOCK_JOIN */
                        0,  /* IP_SUPPORT_XA */
                        1,  /* IP_SUPPORT_QUERY_MODE_SELECTION */
                        0,  /* IP_SUPPORT_VALIDATE_SCHEMAOBJECTS_IN_USE */
                        1,  /* IP_SUPPORT_UNICODE_INFO */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0   /* Reserved for future use */
                    };


    public OpenAccessPassThruIP()
    {
        m_tmHandle = 0;
        tableInfo = new TableInfo();
        restHelper = new RESTHelper();
        oa_ipHelper = new OA_IPHelper();
        oaipDataProcessor = new OAIPDataProcessor();
        m_stmtDA = new XXX_STMT_DA[50];

	}

    public String ipGetInfo(int iInfoType)
    {
		String str = null;

        jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipGetInfo called\n");

        return str;
   }

    public int ipSetInfo(int iInfoType,String InfoVal)
    {
        jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipSetInfo called\n");
        return IP_SUCCESS;
    }

    public int ipGetSupport(int iSupportType)
    {
        jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipGetSupport called\n");
        return(ip_support_array[iSupportType]);
    }

   /*ipConnect is called immediately after an instance of this object is created. You should
    *perform any tasks related to connecting to your data source */
    public int ipConnect(long tmHandle,long dam_hdbc,String sDataSourceName, String sUserName, String sPassword,
						String sCurrentCatalog, String sIPProperties, String sIPCustomProperties)
    {
	jdam.dam_setOption(DAM_CONN_OPTION, dam_hdbc, DAM_CONN_OPTION_POST_PROCESSING, DAM_PROCESSING_OFF);

        /* Save the trace handle */
        m_tmHandle = tmHandle;
        jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipConnect called\n");
        /* Code to connect to your data source source. */

        m_tmHandle = tmHandle;
        jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipConnect called\n");


        //Read baseURL from Custom Properties set in ODBC/JDBC config, if its not set return failure
        String[] customProperties = sIPCustomProperties.split("=");
        if(customProperties[0].equalsIgnoreCase("baseurl"))
        {
            baseURL = customProperties[1];
        }
        else
        {
            return IP_FAILURE;
        }

        //Extract Resource Unique ID for later steps
        int lastindex =  baseURL.lastIndexOf("/");
        int lastindex2 = baseURL.lastIndexOf(".");
        resourceUniqueId = baseURL.substring(lastindex + 1, lastindex2);

        //check the connection to API endpoint, if unsuccessful, return failure
        try {
            ArrayList<String> response = restHelper.getResponseFromAPI(baseURL, m_tmHandle);

            int responseStatusCode = Integer.parseInt(response.get(0));
            if(responseStatusCode != 200)
            {
                if(responseStatusCode == 401 )
                {
                    jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipConnect():Error in connect\n");
                    jdam.dam_addError(dam_hdbc, 0, 1, 0, "Unable to authenticate, please check your credentials \n");
                }
                else if(responseStatusCode == 500)
                {
                    jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipConnect():Error in connect\n");
                    jdam.dam_addError(dam_hdbc, 0, 1, 0, "Server threw an exception\n");
                }
                return IP_FAILURE;
            }
        }
        catch (MalformedURLException ex)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipConnect():Error in connect\n");
            jdam.dam_addError(dam_hdbc, 0, 1, 0, "Invalid endpoint: The URL that you have provided is not a valid URL. \n" + ex.getMessage());
            return IP_FAILURE;
        }
        catch (java.lang.Exception ex)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipConnect():Error in connect\n");
            jdam.dam_addError(dam_hdbc, 0, 1, 0, "Exception: " + ex.getMessage());
            return IP_FAILURE;
        }

        return IP_SUCCESS;
    }

    public int ipDisconnect(long dam_hdbc)
    {   /* disconnect from the data source */
            jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipDisonnect called\n");
            return IP_SUCCESS;
    }

    public int ipStartTransaction(long dam_hdbc)
    {
            /* start a new transaction */
            jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipStartTransaction called\n");
            return IP_SUCCESS;
    }

    public int ipEndTransaction(long dam_hdbc,int iType)
    {
            /* end the transaction */
            jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipEndTransaction called\n");
            if (iType == DAM_COMMIT)
            {
            }
            else if (iType == DAM_ROLLBACK)
            {
            }
            return IP_SUCCESS;
    }

    public int ipExecute(long dam_hstmt, int iStmtType, long hSearchCol,xo_long piNumResRows) {
        jdam.trace(m_tmHandle, UL_TM_F_TRACE, "ipExecute called\n");

        int iRetCode;
        StringBuffer sSqlString, strInfo, whereCond, orderbyCond, groupbyCond, havingCond;
        XXX_STMT_DA     pStmtDA = null;
        int idx = -1;


        sSqlString = new StringBuffer(MAX_QUERY_LEN);
        whereCond = new StringBuffer(MAX_QUERY_LEN);
        orderbyCond = new StringBuffer(MAX_QUERY_LEN);
        groupbyCond = new StringBuffer(MAX_QUERY_LEN);
        havingCond = new StringBuffer(MAX_QUERY_LEN);
        strInfo = new StringBuffer(ip.DAM_MAX_ID_LEN + 1);
        SQLTreeParser sqlTreeParser = new SQLTreeParser(true, true, m_tmHandle);

        if (iStmtType == DAM_SELECT)
        {
            long    hrow = 0;
            xo_int  piValue;

            pStmtDA = new XXX_STMT_DA();
            pStmtDA.dam_hstmt = dam_hstmt;
            pStmtDA.iType = iStmtType;


            try {
                sqlTreeParser.ip_format_query(jdam.damex_getQuery(dam_hstmt), sSqlString);
            } catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipExecute(): Exception in formatting. " + e + "\n");
            }

            HashMap<String, String> conditions = sqlTreeParser.getConditions();
            String queryURL = restHelper.buildQueryURL(conditions, baseURL);
            ArrayList<String> dataFromAPI = null;


            try {
                dataFromAPI = restHelper.getResponseFromAPI(queryURL, m_tmHandle);
            }
            catch (Exception ex)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipExecute():Error while fetching data from API\n" + ex.getMessage());
                return DAM_FAILURE;
            }



            idx = getStmtIndex();
            if(idx >= 0) {
                m_stmtDA[idx] = pStmtDA;
            }

            jdam.dam_setIP_hstmt(dam_hstmt, idx); /* save the StmtDA index*/

            /* get fetch block size */
            piValue = new xo_int();
            iRetCode = jdam.dam_getInfo(0, pStmtDA.dam_hstmt, DAM_INFO_FETCH_BLOCK_SIZE,
                    strInfo, piValue);
            if (iRetCode != DAM_SUCCESS)
                pStmtDA.iFetchSize = 100;
            else
                pStmtDA.iFetchSize = piValue.getVal();


            oaipDataProcessor.process_rows(pStmtDA, dataFromAPI, conditions.get("select"), tableInfo);

        }

        return IP_SUCCESS;

    }

    /* this example uses static schema and only SELECT command supported, so following functions are not called */
    public int ipSchema(long dam_hdbc,long pMemTree,int iType, long pList, Object pSearchObj)
    {
			jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipSchema called\n");
			switch(iType)
			{
			case DAMOBJ_TYPE_CATALOG:
				{
					schemaobj_table TableObj = new schemaobj_table(OA_CATALOG_NAME,null,null,null,null,null,null,null);

					jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);
				}
				break;
			case DAMOBJ_TYPE_SCHEMA:
				{
					schemaobj_table TableObj = new schemaobj_table();

					TableObj.SetObjInfo(null,"SYSTEM",null,null,null,null,null,null);
					jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);

					TableObj.SetObjInfo(null,OA_USER_NAME,null,null,null,null,null,null);
					jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);
				}
				break;
			case DAMOBJ_TYPE_TABLETYPE:
				{
					schemaobj_table TableObj = new schemaobj_table();

					TableObj.SetObjInfo(null,null,null,"SYSTEM TABLE",null,null,null,null);
					jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);

					TableObj.SetObjInfo(null,null,null,"TABLE",null,null,null,null);
					jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);

					TableObj.SetObjInfo(null,null,null,"VIEW",null,null,null,null);
					jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);
				}

				break;
			case DAMOBJ_TYPE_TABLE:
				{
                    schemaobj_table  pTableSearchObj = (schemaobj_table) pSearchObj;

                    //If user is querying on a table, then pTableSearchObj will not be null. You would have to check if the table indeed exists and send the info about table.
                    if (pTableSearchObj != null)
                    {
                        jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Dynamic Schema  of table:<"+pTableSearchObj.getTableQualifier()+"."+pTableSearchObj.getTableOwner()+"."+pTableSearchObj.getTableName()+"> is being requested\n");
                        if(pTableSearchObj.getTableName().equalsIgnoreCase("NYCOPENDATA")) {
                            schemaobj_table TableObj = new schemaobj_table();
                            TableObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "NYCOPENDATA", "TABLE", null, null, "0x0F", null);
                            tableInfo.addnewTable("NYCOPENDATA");
                            jdam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, TableObj);
                        }

                    }
                    //Else Add all tables
                    //As we are dealing with only one table here, the code is same for these two conditions in this case.
                    else
                    {
                        schemaobj_table TableObj = new schemaobj_table();
                        TableObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "NYCOPENDATA", "TABLE", null, null, "0x0F", null);
                        jdam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, TableObj);
                        tableInfo.addnewTable("NYCOPENDATA");
                        jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Dynamic Schema for all tables is being requested\n");

                    }
				}
				break;
			case DAMOBJ_TYPE_COLUMN:
				{
                    schemaobj_column pColSearchObj = (schemaobj_column) pSearchObj;
                    //If user is querying on a table, then pTableSearchObj will not be null. If its not null you would have to send the columns for the table requested else you need to send all columns for all tables
                    //In the example, we have only 1 table, so the code will be the same for both the conditions
                    if (pColSearchObj != null)
                    {
                        HashMap<String, Integer> columnMeta = null;
                        if(tableInfo.getColumnsinTable(pColSearchObj.getTableName().toUpperCase()) == null) {
                            columnMeta = restHelper.getMetadata(resourceUniqueId, m_tmHandle);
                            tableInfo.addcolumnstoTable(pColSearchObj.getTableName().toUpperCase(), columnMeta);
                        } else
                        {
                            columnMeta = tableInfo.getColumnsinTable(pColSearchObj.getTableName().toUpperCase());
                        }
                        for(Map.Entry<String, Integer> currentcolumn: columnMeta.entrySet())
                        {
                            schemaobj_column oa_column = oa_ipHelper.add_column_meta(currentcolumn, OA_CATALOG_NAME, OA_USER_NAME);
                            jdam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, oa_column);
                        }

                        jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Dynamic Schema for column <"+pColSearchObj.getColumnName()+"> of table:<"+pColSearchObj.getTableQualifier()+"."+pColSearchObj.getTableOwner()+"."+pColSearchObj.getTableName()+"> is being requested\n");
                    }
                    else
                    {
                        HashMap<String, Integer> columnMeta = null;
                        if( tableInfo.getColumnsinTable(pColSearchObj.getTableName().toUpperCase()) == null ) {
                            columnMeta = restHelper.getMetadata(resourceUniqueId, m_tmHandle);
                            tableInfo.addcolumnstoTable(pColSearchObj.getTableName().toUpperCase(), columnMeta);
                        }
                        for(Map.Entry<String, Integer> currentcolumn: columnMeta.entrySet())
                        {
                            schemaobj_column oa_column =oa_ipHelper.add_column_meta(currentcolumn, OA_CATALOG_NAME, OA_USER_NAME);
                            jdam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, oa_column);
                        }
                        jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Dynamic Schema for all columns of all tables is being requested\n");
                    }
				}
				break;

			case DAMOBJ_TYPE_STAT:
				break;

			case DAMOBJ_TYPE_FKEY:
				break;
			case DAMOBJ_TYPE_PKEY:
				break;
			case DAMOBJ_TYPE_PROC:
				break;
			case DAMOBJ_TYPE_PROC_COLUMN:
				break;
			default:
				break;
			}
			return IP_SUCCESS;
    }

    public int        ipDDL(long dam_hstmt, int iStmtType, xo_long piNumResRows)
    {
			jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipDDL called\n");
            return IP_FAILURE;
    }

    public int        ipProcedure(long dam_hstmt, int iType, xo_long piNumResRows)
    {
			jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipProcedure called\n");
            return IP_FAILURE;
    }

    public int        ipDCL(long dam_hstmt, int iStmtType, xo_long piNumResRows)
    {
			jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipProcedure called\n");
            return IP_FAILURE;
    }

    public int        ipPrivilege(int iStmtType,String pcUserName,String pcCatalog,String pcSchema,String pcObjName)
    {
			jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipPrivilege called\n");
			return IP_FAILURE;
    }

    public int        ipNative(long dam_hstmt, int iCommandOption, String sCommand, xo_long piNumResRows)
    {
			jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipNative called\n");
            return IP_FAILURE;
    }

    public int        ipSchemaEx(long dam_hstmt, long pMemTree, int iType, long pList,Object pSearchObj)
	{
			jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipSchemaEx called\n");
            return IP_FAILURE;
    }

    public int        ipProcedureDynamic(long dam_hstmt, int iType, xo_long piNumResRows)
    {
			jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipProcedureDynamic called\n");
            return IP_FAILURE;
    }

    public int getStmtIndex()
    {
        for (int i=0; i < m_stmtDA.length; i++) {
            if(m_stmtDA[i] == null)
                return i;
        }
        return -1;
    }

}
