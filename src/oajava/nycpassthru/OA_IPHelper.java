package oajava.nycpassthru;

/**
 * Created by sbobba on 9/8/2017.
 */
import oajava.sql.*;
import java.util.Map;

import static oajava.sql.ip.*;

public class OA_IPHelper {


    oa_types_info[] getTypesSupported()
    {
        int i = 0;
        oa_types_info[] typesInfo = new oa_types_info[22];
        typesInfo[i++] = new oa_types_info("CHAR", 1, 4096, "'", "'", "length", 1, 1, 3, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "CHAR");
        typesInfo[i++] = new oa_types_info("NUMERIC", 2, 40, null, null, "precision,scale", 1, 0, 2, 0, 0, 0, 0, 32, "NUMERIC");
        typesInfo[i++] = new oa_types_info("DECIMAL", 2, 40, null, null, "precision,scale", 1, 0, 2, 0, 0, 0, 0, 32, "DECIMAL");
        typesInfo[i++] = new oa_types_info("INTEGER", 4, 10, null, null, null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "INTEGER");
        typesInfo[i++] = new oa_types_info("BIGINT", XO_TYPE_BIGINT, 19, null, null, null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "BIGINT");
        typesInfo[i++] = new oa_types_info("SMALLINT", 5, 5, null, null, null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "SMALLINT");
        typesInfo[i++] = new oa_types_info("REAL", 7, 7, null, null, null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "REAL");
        typesInfo[i++] = new oa_types_info("DOUBLE", 8, 15, null, null, null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "DOUBLE");
        typesInfo[i++] = new oa_types_info("BINARY", -2, 4096, "0x", null, "length", 1, 0, 0, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "BINARY");
        typesInfo[i++] = new oa_types_info("VARBINARY", -3, 4096, "0x", null, "max length", 1, 0, 0, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "VARBINARY");
        typesInfo[i++] = new oa_types_info("LONGVARBINARY", -4, 2147483647, "0x", null, "max length", 1, 0, 0, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "LONGVARBINARY");
        typesInfo[i++] = new oa_types_info("VARCHAR", 12, 4096, "'", "'", "max length", 1, 1, 3, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "VARCHAR");
        typesInfo[i++] = new oa_types_info("LONGVARCHAR", -1, 2147483647, "'", "'", "max length", 1, 1, 3, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "LONGVARCHAR");
        typesInfo[i++] = new oa_types_info("DATE", 91, 10, "'", "'", null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "DATE");
        typesInfo[i++] = new oa_types_info("TIME", 92, 8, "'", "'", null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "TIME");
        typesInfo[i++] = new oa_types_info("TIMESTAMP", 93, 19, "'", "'", null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "TIMESTAMP");
        typesInfo[i++] = new oa_types_info("BIT", XO_TYPE_BIT, 1, null, null, null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "BIT");
        typesInfo[i++] = new oa_types_info("TINYINT", XO_TYPE_TINYINT, 3, null, null, null, 1, 0, 2, 1, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "TINYINT");
        typesInfo[i++] = new oa_types_info("NULL", XO_TYPE_NULL, 1, null, null, null, 1, 0, 2, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "NULL");
        typesInfo[i++] = new oa_types_info("WCHAR", -8, 4096, "N'", "'", "length", 1, 1, 3, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "WCHAR");
        typesInfo[i++] = new oa_types_info("WVARCHAR", -9, 4096, "N'", "'", "max length", 1, 1, 3, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "WVARCHAR");
        typesInfo[i] = new oa_types_info("WLONGVARCHAR", -10, 2147483647, "N'", "'", "max length", 1, 1, 3, 0, 0, 0, DAMOBJ_NOTSET, DAMOBJ_NOTSET, "WLONGVARCHAR");

        return typesInfo;
    }

    int map_REST_to_OA_datatypes(String text)
    {
        switch(text){
            case "text":
                return 12;

            case "number":
                return -5;

            case "calendar_date":
                return 93;

            default:
                return 12;

        }
    }

    schemaobj_column add_column_meta(Map.Entry<String, Integer> currentcolumn, String OA_CATALOG_NAME, String OA_USER_NAME)
    {
        schemaobj_column oa_column = new schemaobj_column();

        //If datatype is Numeric
        if(currentcolumn.getValue() == -5)
        {
            oa_column.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "NYCOPENDATA", currentcolumn.getKey().toUpperCase(), (short) XO_TYPE_BIGINT, "BIGINT", 0, 40, (short)DAMOBJ_NOTSET, (short)DAMOBJ_NOTSET, (short)XO_NULLABLE, (short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,null );
        }
        //If datatype is VARCHAR
        else if(currentcolumn.getValue() == 12)
        {
            oa_column.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "NYCOPENDATA", currentcolumn.getKey().toUpperCase(), (short)XO_TYPE_VARCHAR, "VARCHAR", 4096, 0, (short)DAMOBJ_NOTSET, (short)0, (short)XO_NULLABLE, (short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,null );
        }
        //If datatype is TIMESTAMP
        else if(currentcolumn.getValue() == 93)
        {
            oa_column.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "NYCOPENDATA", currentcolumn.getKey().toUpperCase(), (short)XO_TYPE_TIMESTAMP, "TIMESTAMP", 19, 0, (short)DAMOBJ_NOTSET, (short)0, (short)XO_NULLABLE, (short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,null );
        }

        return oa_column;
    }

}
