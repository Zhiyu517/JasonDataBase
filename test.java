import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class test {
    @Test
    public void testCreateDataBase() {
        DataBaseJason myDataBases = new DataBaseJason();
        DB curDataBase = myDataBases.createDataBase("School");
        Assert.assertEquals("School", curDataBase.getDataBaseName());
    }

    @Test
    public void testCreateTable() {
        DataBaseJason myDataBases = new DataBaseJason();
        DB curDataBase = myDataBases.createDataBase("School");
        String[][] columnNameAndType = new String[][]{{"StudentId", "Varchar"}, {"Birthday", "Varchar"}, {"Gender", "Varchar"}};
        Table studentTable = curDataBase.createTable("Students", columnNameAndType);

        Map<String, String> testStudent = new HashMap<>();
        testStudent.put("StudentId", "Varchar");
        testStudent.put("Birthday", "Varchar");
        testStudent.put("Gender", "Varchar");
        Assert.assertEquals(testStudent, studentTable.getColoInformation());
    }

    @Test
    public void testInsertRow() {
        DataBaseJason myDataBases = new DataBaseJason();
        DB curDataBase = myDataBases.createDataBase("School");
        String[][] columnNameAndType = new String[][]{{"StudentId", "Varchar"}, {"Birthday", "Varchar"}, {"Gender", "Varchar"}};
        Table studentTable = curDataBase.createTable("Students", columnNameAndType);
        Map<String, String> testStudent = new HashMap<>();
        testStudent.put("StudentId", "Varchar");
        testStudent.put("Birthday", "Varchar");
        testStudent.put("Gender", "Varchar");


        String[][] column = new String[][]{{"StudentId", "001"}, {"Birthday", "1995-01-01"}, {"Gender", "Male"}};
        studentTable.insertRow(column);
        Map<String, String> testRow = new HashMap<>();
        testRow.put("StudentId", "001");
        testRow.put("Birthday", "1995-01-01");
        testRow.put("Gender", "Male");
        testRow.put("rowKey", "1");
        Assert.assertEquals(testRow, studentTable.getAllRows().get(1).getRow());
    }

    @Test
    public void testDeleteRow() {
        DataBaseJason myDataBases = new DataBaseJason();
        DB curDataBase = myDataBases.createDataBase("School");
        String[][] columnNameAndType = new String[][]{{"StudentId", "Varchar"}, {"Birthday", "Varchar"}, {"Gender", "Varchar"}};
        Table studentTable = curDataBase.createTable("Students", columnNameAndType);


        String[][] column = new String[][]{{"StudentId", "001"}, {"Birthday", "1995-01-01"}, {"Gender", "Male"}};
        studentTable.insertRow(column);
        studentTable.deleteRow(1);
        Assert.assertEquals(false, studentTable.getAllRows().containsKey(1));
    }

    @Test
    public void testUpdateRow() {
        DataBaseJason myDataBases = new DataBaseJason();
        DB curDataBase = myDataBases.createDataBase("School");
        String[][] columnNameAndType = new String[][]{{"StudentId", "Varchar"}, {"Birthday", "Varchar"}, {"Gender", "Varchar"}};
        Table studentTable = curDataBase.createTable("Students", columnNameAndType);
        String[][] column = new String[][]{{"StudentId", "001"}, {"Birthday", "1995-01-01"}, {"Gender", "Male"}};
        studentTable.insertRow(column);

        Map<String, String> testRow = new HashMap<>();
        testRow.put("StudentId", "001");
        testRow.put("Birthday", "1995-01-01");
        testRow.put("Gender", "Female");
        testRow.put("rowKey", "1");
        String[] updatedMessage = new String[]{"Gender", "Female"};
        studentTable.updateRow(1, updatedMessage);
        Assert.assertEquals(testRow, studentTable.getAllRows().get(1).getRow());
    }

    @Test
    public void testgetAllRows() {
        DataBaseJason myDataBases = new DataBaseJason();
        DB curDataBase = myDataBases.createDataBase("School");
        String[][] columnNameAndType = new String[][]{{"StudentId", "Varchar"}, {"Birthday", "Varchar"}, {"Gender", "Varchar"}};
        Table studentTable = curDataBase.createTable("Students", columnNameAndType);
        String[][] column = new String[][]{{"StudentId", "001"}, {"Birthday", "1995-01-01"}, {"Gender", "Male"}};
        studentTable.insertRow(column);
        String[][] column2 = new String[][]{{"StudentId", "002"}, {"Birthday", "1995-01-02"}, {"Gender", "Female"}};
        studentTable.insertRow((column2));
        List<Map<String, String>> expected = new ArrayList<>();
        Map<String, String> testRow = new HashMap<>();
        testRow.put("StudentId", "001");
        testRow.put("Birthday", "1995-01-01");
        testRow.put("Gender", "Male");
        testRow.put("rowKey", "1");
        Map<String, String> testRow2 = new HashMap<>();
        testRow2.put("StudentId", "002");
        testRow2.put("Birthday", "1995-01-02");
        testRow2.put("Gender", "Female");
        testRow2.put("rowKey", "2");
        expected.add(testRow);
        expected.add(testRow2);
        Map<Integer, DbRow> allRows = studentTable.getAllRows();


        int size = allRows.size();
        Assert.assertEquals(size ,2);
        int index = 0;
        for (Map.Entry<Integer, DbRow> myEntry : allRows.entrySet()) {
            Assert.assertEquals(expected.get(index) ,myEntry.getValue().getRow());
            index++;
        }
    }

    @Test
    public void testFirstKRows() {
        DataBaseJason myDataBases = new DataBaseJason();
        DB curDataBase = myDataBases.createDataBase("School");
        String[][] columnNameAndType = new String[][]{{"StudentId", "Varchar"}, {"Birthday", "Varchar"}, {"Gender", "Varchar"}};
        Table studentTable = curDataBase.createTable("Students", columnNameAndType);
        String[][] column = new String[][]{{"StudentId", "001"}, {"Birthday", "1995-01-01"}, {"Gender", "Male"}};
        studentTable.insertRow(column);
        String[][] column2 = new String[][]{{"StudentId", "002"}, {"Birthday", "1995-01-02"}, {"Gender", "Female"}};
        studentTable.insertRow((column2));
        List<Map<String, String>> expected = new ArrayList<>();
        Map<String, String> testRow = new HashMap<>();
        testRow.put("StudentId", "001");
        testRow.put("Birthday", "1995-01-01");
        testRow.put("Gender", "Male");
        testRow.put("rowKey", "1");
        Map<String, String> testRow2 = new HashMap<>();
        testRow2.put("StudentId", "002");
        testRow2.put("Birthday", "1995-01-02");
        testRow2.put("Gender", "Female");
        testRow2.put("rowKey", "2");
        expected.add(testRow);
        expected.add(testRow2);

        Map<Integer, DbRow> first2Row = studentTable.firstKRows(2);
        int size = first2Row.size();
        Assert.assertEquals(size ,2);
        int index = 0;
        for (Map.Entry<Integer, DbRow> myEntry : first2Row.entrySet()) {
            Assert.assertEquals(expected.get(index) ,myEntry.getValue().getRow());
            index++;
        }

    }

    @Test
    public void testSortBy() {
        DataBaseJason myDataBases = new DataBaseJason();
        DB curDataBase = myDataBases.createDataBase("School");
        String[][] columnNameAndType = new String[][]{{"StudentId", "Varchar"}, {"Birthday", "Varchar"}, {"Gender", "Varchar"}};
        Table studentTable = curDataBase.createTable("Students", columnNameAndType);
        String[][] column = new String[][]{{"StudentId", "004"}, {"Birthday", "1995-01-01"}, {"Gender", "Male"}};
        studentTable.insertRow(column);
        String[][] column2 = new String[][]{{"StudentId", "002"}, {"Birthday", "1995-01-02"}, {"Gender", "Female"}};
        studentTable.insertRow((column2));
        List<Map<String, String>> expected = new ArrayList<>();
        Map<String, String> testRow = new HashMap<>();
        testRow.put("StudentId", "004");
        testRow.put("Birthday", "1995-01-01");
        testRow.put("Gender", "Male");
        testRow.put("rowKey", "1");
        Map<String, String> testRow2 = new HashMap<>();
        testRow2.put("StudentId", "002");
        testRow2.put("Birthday", "1995-01-02");
        testRow2.put("Gender", "Female");
        testRow2.put("rowKey", "2");
        expected.add(testRow2);
        expected.add(testRow);

        List<DbRow> sortedRows = studentTable.sortBy("StudentId", 2);
        int size = sortedRows.size();
        Assert.assertEquals(size ,2);
        int index = 0;
        for (DbRow eachDb : sortedRows) {
            Assert.assertEquals(expected.get(index) , eachDb.getRow());
            index++;
        }
    }


    @Test
    public void testGroupBy() {
        DataBaseJason myDataBases = new DataBaseJason();
        DB curDataBase = myDataBases.createDataBase("School");
        String[][] columnNameAndType = new String[][]{{"StudentId", "Varchar"}, {"Birthday", "Varchar"}, {"Gender", "Varchar"}};
        Table studentTable = curDataBase.createTable("Students", columnNameAndType);
        String[][] column = new String[][]{{"StudentId", "004"}, {"Birthday", "1995-01-01"}, {"Gender", "Male"}};
        studentTable.insertRow(column);
        String[][] column2 = new String[][]{{"StudentId", "002"}, {"Birthday", "1995-01-02"}, {"Gender", "Male"}};
        studentTable.insertRow((column2));
        List<Object[]> expect = new ArrayList<>();
        expect.add(new Object[]{"Male", 2});
        List<Object[]> groupByResult = studentTable.groupBy("Gender", 2);
        Assert.assertEquals(1, groupByResult.size());
        for (int i = 0; i < groupByResult.size(); i++) {
            Assert.assertEquals(expect.get(i), groupByResult.get(i));
        }
    }

}
