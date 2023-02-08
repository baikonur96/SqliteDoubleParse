import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class Requests {

    public static Map<String, StringBuilder> reqSqliteAnaBin() {
        long start = System.currentTimeMillis();
        Map<String, StringBuilder> mapAna = new LinkedHashMap<>();
        Map<String, StringBuilder> mapBin = new LinkedHashMap<>();
        String urlSqLite = "jdbc:sqlite:C:/Users/Adminsvu/Documents/BestProjectEverSrvpe/ioprs/ioprs.db.1608700612";


        try {
            Connection connectionSqLite = DriverManager.getConnection(urlSqLite);
            if (connectionSqLite != null) {
                System.out.println("connectedSqLite");
            }

            Statement statementSqLite = connectionSqLite.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY
            );


            long startAna = System.currentTimeMillis();
            ResultSet resSecValAna = statementSqLite.executeQuery("select `sig_id`, `sec`, `val`\n" +
                    "from `arch_ana`\n");
            while (resSecValAna.next()) {
                if (mapAna.containsKey(resSecValAna.getString("sig_id"))) {
                    mapAna.get(resSecValAna.getString("sig_id")).append
                            ("('" + resSecValAna.getString("sec") + "', '" + resSecValAna.getString("val") + "'), ");
                } else {
                    StringBuilder builder = new StringBuilder("INSERT INTO `" + resSecValAna.getString("sig_id") + "` (`sec`, `val`) VALUES ");
                    builder.append("('" + resSecValAna.getString("sec") + "', '" + resSecValAna.getString("val") + "'), ");
                    mapAna.put(resSecValAna.getString("sig_id"), builder);
                }
            }
            System.out.println("Ana выполнено. Время выполнения: " + (System.currentTimeMillis() - startAna));

            long startBin = System.currentTimeMillis();
            ResultSet resSecValBin = statementSqLite.executeQuery("select `sig_id`, `sec`, `val`\n" +
                    "from `arch_dis`\n");
            while (resSecValBin.next()) {
                if (mapBin.containsKey(resSecValBin.getString("sig_id"))) {
                    mapBin.get(resSecValBin.getString("sig_id")).append
                            ("('" + resSecValBin.getString("sec") + "', '" + resSecValBin.getString("val") + "'), ");
                } else {
                    StringBuilder builder = new StringBuilder("INSERT INTO `" + resSecValBin.getString("sig_id") + "` (`sec`, `val`) VALUES ");
                    builder.append("('" + resSecValBin.getString("sec") + "', '" + resSecValBin.getString("val") + "'), ");
                    mapBin.put(resSecValBin.getString("sig_id"), builder);

                }
            }
            System.out.println("Bin выполнено. Время выполнения: " + (System.currentTimeMillis() - startBin));
            statementSqLite.close();
            connectionSqLite.close();

            mapAna.putAll(mapBin);


            System.out.println("Время работы по преобразованию одного файла ioprs.db: " + (System.currentTimeMillis() - start));
        } catch (Exception e) {
            e.printStackTrace();
        }


        return mapAna;
    }


    public static void reqNewSqliteAnaBin(Map<String, StringBuilder> inputMap) {
        int count = 0;
        int sizeInputMap = inputMap.size();
        String newUrlSqLite = "jdbc:sqlite:C:/Users/Adminsvu/Documents/Data/NewSqliteDb/ioprs.db.1608682612";
        long startReqNewSqliteAnaBin = System.currentTimeMillis();

        try {

            Connection connectionNewSqLite = DriverManager.getConnection(newUrlSqLite);
            if (connectionNewSqLite != null) {
                System.out.println("connectedNewSqLite");
            }
            Statement statementNewSqLite = connectionNewSqLite.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY
            );
            for (Map.Entry<String, StringBuilder> entry : inputMap.entrySet()) {

                long startCikl = System.currentTimeMillis();
                count++;
                statementNewSqLite.executeUpdate("CREATE TABLE IF NOT EXISTS `" + entry.getKey() + "` (\n" +
                        "`sec` int,\n" +
                        "`val` float\n" +
                        ");\n");
                StringBuilder builder = new StringBuilder(entry.getValue());
                builder.delete(builder.length() - 2, builder.length() - 1);
                builder.insert(builder.length() - 1, ";");
                statementNewSqLite.executeUpdate(builder.toString());
                System.out.println(entry.getKey() + " - Файл - " + count + "/" + sizeInputMap + "\n" +
                        "Время создания и заполнение таблицы - " + (System.currentTimeMillis() - startCikl));

            }
            System.out.println("Время создания и заполнения полного файла ioprs.db: " + (System.currentTimeMillis() - startReqNewSqliteAnaBin));
        } catch (Exception c) {
            c.printStackTrace();
        }
    }

}
