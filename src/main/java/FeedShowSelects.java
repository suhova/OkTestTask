import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lead;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;


public class FeedShowSelects {
    public static SparkSession spark;

    public static void main(String[] args) {
        spark = createSparkSession();
        // таблицы с входными данными
        Dataset<Row> feedsShow = spark.read().json("src/main/resources/feeds_show2.json");
        Dataset<Row> feedOwners = spark.read().json("src/main/resources/feed_owners.json");
        Dataset<Row> feedResources = spark.read().json("src/main/resources/feed_resources.json");
        // список пользователей для примера
        Dataset<Row> users = feedsShow.select("userId").withColumnRenamed("userId", "id").limit(10).distinct();
        select1(feedsShow);
        select2(feedOwners, feedResources);
        select3(feedsShow);
        select4(feedsShow, users);
    }

    /*
     * Количество показов и уникальных пользователей за день в разрезе по платформам,
     * в том числе по всем платформам суммарно
     * ************************************************************************
     * Сложность - О(n)
     */
    static void select1(Dataset<Row> feedsShow) {
        List<String> platformList = Arrays.asList("APP_ANDROID", "APP_IOS", "APP_WINPHONE", "DESKTOP_WEB", "MOBILE_WEB");
        Dataset<String> platforms = spark.createDataset(platformList, Encoders.STRING());
        feedsShow.rollup("platform")
                .agg(count("feedId").as("shows"),
                        countDistinct("userId").as("uniqueUsers"))
                //на случай отсутствующих платформ (кейс выглядит практически недстижимым, но сейчас для красоты 6 строк можно и сджойнить за константу)
                .join(platforms, col("platform").equalTo(col("value")), "outer")
                .select(col("value").as("platform"), col("shows"), col("uniqueUsers"))
                .na().fill(ImmutableMap.of("platform", "ALL", "shows", 0, "uniqueUsers", 0))
                .orderBy("shows")
                .show();
    }

    /*
     * Количество уникальных авторов и уникального контента, показанного в ленте
     ***********************************************************************************
     * Сложность - О(n), где n - суммарное кол-во записей в таблицах feedOwners и feedResources
     */
    static void select2(Dataset<Row> feedOwners, Dataset<Row> feedResources) {
        feedOwners//.select("ownerId", "ownerType")
                .agg(countDistinct("ownerId", "ownerType").as("countOfUniqueOwners"))
                .crossJoin(feedResources.agg(countDistinct("resourceId", "resourceType").as("countOfUniqueContent")))
                .show();
    }

    /*
     * Количество сессий, средняя глубина просмотра (по позиции фида)
     * и средняя продолжительность пользовательской сессии в ленте за день.
     *
     * Пусть пользовательская сессия заканчивается либо там, где в ленте следующим после фида на n-ой позиции
     * был просмотрен фид на позиции 1, либо просмотров больше не было. (порядок просмотров пользователя определаем по timestamp)
     ************************************************************************
     * Сложность - O(n)
     * Формально сложность = О(n), но n имеет большой коэффициент, т.к.
     * O(n) <= (сортировка за log(n) + case с оконными функциями за n + sum("durationMs") за n + where условие за n + count и avg за n)
     ************************************************************************
     * Сложность можно уменшить (уменьшить коэффициент перед n), помечая при логировании фид, на котором пользователь завершал сессию просмотра ленты.
     * Это позволит избавиться от блока when(...), в котором мы полным проходом по таблице выбираем предполагаемые последние фиды в сессии.
     */
    static void select3(Dataset<Row> feedsShow) {
        WindowSpec ws = Window.orderBy("userId", "timestamp", "position");
        feedsShow.select(
                when(lead("position", 1, 1).over(ws).equalTo(1)
                        .or(lead("userId", 1).over(ws).notEqual(col("userId")))
                        .or(lead("platform", 1).over(ws).notEqual(col("platform"))), col("position"))
                        .otherwise(0).as("pos"),
                sum("durationMs").over().as("sum"))
                .where(col("pos").notEqual(0))
                .select(count("pos").as("sessions"),
                        avg("pos").as("avgSessionsDeep"),
                        first("sum").as("sum"))
                .select(col("sessions"),
                        col("avgSessionsDeep"),
                        col("sum").divide(col("sessions")).as("avgSessionDurationMs")).show();
    }

    /*
     * Дополнительно дан список пользователей в формате <userId: Long>, включающий в себе несколько тысяч пользователей.
     * Нужно посчитать количество показов фидов в ленте по этим пользователям.
     * ************************************************************************************************
     * Сложность - О(n);
     * (broadcast hash join за O(n) и count("feedId") тоже за n)
     */
    static void select4(Dataset<Row> feedsShow, Dataset<Row> users) {
        feedsShow.join(users.hint("broadcast"), users.col("id").equalTo(feedsShow.col("userID")), "right")
                .groupBy("id")
                .agg(count("feedId").as("shows"))
                .show();
    }

    static SparkSession createSparkSession() {
        System.setProperty("hadoop.home.dir", "D:\\winutils\\");
        return SparkSession
                .builder()
                .config("spark.master", "local")
                .getOrCreate();
    }
}
