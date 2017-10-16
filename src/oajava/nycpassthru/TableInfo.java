package oajava.nycpassthru;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by sbobba on 9/8/2017.
 */
public class TableInfo {

    private HashMap<String, HashMap<String, Integer>> tableData = null;

    public TableInfo()
    {
        tableData = new HashMap<>();
    }


    void addnewTable(String tableName)
    {
        if(!tableData.containsKey(tableName.toUpperCase())) {
            tableData.put(tableName.toUpperCase(), null);
        }
    }

    void addcolumnstoTable(String tableName, HashMap<String, Integer> columnInfo)
    {
        if(tableData.containsKey(tableName.toUpperCase()) && tableData.get(tableName.toUpperCase()) == null)
        {
            tableData.put(tableName.toUpperCase(), columnInfo);
        }
    }

    ArrayList<String> getAllTables()
    {
        ArrayList<String> tablesAvailable = new ArrayList<>();


        for(String table: tableData.keySet())
        {
            tablesAvailable.add(table);
        }

        return  tablesAvailable;
    }

    HashMap<String, Integer> getColumnsinTable(String tableName)
    {
        if(tableData.containsKey(tableName.toUpperCase()))
        {
            return tableData.get(tableName.toUpperCase());
        }
        else
        {
            return null;
        }
    }




}
