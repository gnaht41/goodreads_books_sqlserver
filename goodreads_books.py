import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from pyspark.sql.functions import explode, split, monotonically_increasing_id, col
import os
import shutil
from pyspark.sql.functions import col, count, when
from pyspark.sql.functions import col, current_date, to_date, lit
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import explode, split
from pyspark.sql.functions import monotonically_increasing_id, col, regexp_replace
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, monotonically_increasing_id, explode, split, date_format
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, to_date, monotonically_increasing_id,
    explode, split,  when, trim, collect_list, coalesce, lit, count
)

# Khởi tạo phiên Spark với MongoDB và PostgreSQL
spark = SparkSession.builder \
    .appName("Goodreads Spark with MongoDB and PostgreSQL") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.7.4") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/goodreads_db.books") \
    .getOrCreate()


# Thiết lập chính sách phân tích cú pháp thời gian thành LEGACY để xử lý ngày tháng cũ
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Cài đặt mức độ log: Đây là phương thức để thiết lập mức độ log của Spark. Khi bạn đặt mức log, Spark sẽ chỉ hiển thị các thông tin có mức độ quan trọng ngang hoặc cao hơn mức log đã đặt.
spark.sparkContext.setLogLevel("INFO")

# Đọc dữ liệu từ MongoDB
df = spark.read \
    .format("mongo") \
    .option("uri", "mongodb://localhost:27017/goodreads_db.books") \
    .load()
# Xử lý cột publish_date
df = df.withColumn("cleaned_date", regexp_replace(
    col("Date"), "First published ", ""))

# Chuyển đổi cột cleaned_date với các định dạng khác nhau và xử lý giá trị null
date_formats = [
    "MMMM d, yyyy",
    "yyyy",
    "MMMM yyyy"
]

# Khởi tạo publish_date với giá trị null
df = df.withColumn("publish_date", lit(None))

# Lặp qua các định dạng ngày
for date_format_str in date_formats:
    df = df.withColumn("publish_date",
                       when(col("publish_date").isNull(), to_date(
                           col("cleaned_date"), date_format_str))
                       .otherwise(col("publish_date")))

# Xử lý giá trị null và chuyển đổi cột publish_date
df = df.withColumn("publish_date",
                   coalesce(col("publish_date"), lit("1900-01-01")))

# Chuyển đổi về kiểu dữ liệu date
df = df.withColumn("publish_date", to_date(col("publish_date"), "yyyy-MM-dd"))


# Xử lý dữ liệu có dạng "8,932,568" - Loại bỏ dấu phẩy
df = df.withColumn("Number of Ratings", regexp_replace(col("Number of Ratings"), ",", "")) \
       .withColumn("Reviews", regexp_replace(col("Reviews"), ",", "")) \
       .withColumn("Score", regexp_replace(col("Score"), ",", ""))

# Chuyển đổi kiểu dữ liệu sau khi loại bỏ dấu phẩy
df = df.withColumn("Pages", col("Pages").cast("int")) \
       .withColumn("Rating", col("Rating").cast("float")) \
       .withColumn("Number of Ratings", col("Number of Ratings").cast("int")) \
       .withColumn("Reviews", col("Reviews").cast("int")) \
       .withColumn("Score", col("Score").cast("int"))


# Bước 4: Xử lý trường hợp Description không hợp lệ
df = df.withColumn("Description",
                   F.when(col("Description").isNull() | (F.trim(col("Description"))
                          == ""), "No description available")  # Thay thế NaN và chuỗi rỗng
                   # Thay thế chỉ số
                    .when(col("Description").rlike("^[0-9]+$"), "No description available")
                   # Thay thế chuỗi chỉ có khoảng trắng
                    .when(col("Description").rlike("^[\\s]+$"), "No description available")
                   # Thay thế chuỗi không hợp lệ
                    .when(col("Description").rlike("^[0-9]+[a-zA-Z]+|[a-zA-Z]+[0-9]+$"), "No description available")
                    .otherwise(col("Description")))

# Bước 6: Xóa dấu nháy kép và ký tự không hợp lệ trong Description
df = df.withColumn("Description", regexp_replace(
    col("Description"), '"', ''))  # Xóa dấu nháy kép
df = df.withColumn("Description", regexp_replace(
    col("Description"), '[^a-zA-Z0-9\\s]', ''))  # Xóa ký tự không hợp lệ

# Bước 7: Xóa dữ liệu trùng lặp
df = df.dropDuplicates()

# Bước 8: Xóa cột _id nếu có
df = df.drop("_id")

# Hiển thị DataFrame sau khi xử lý
df.show(truncate=False)


# Kiểm tra và điền giá trị mặc định cho các giá trị null
df = df.na.fill({
    "Rank": 0,
    "Title": "No title",
    "Author": "no author",
    "Rating": 0.0,
    "Number of Ratings": 0,
    "Description": "No description available",
    "Reviews": 0,
    "Pages": 0,
    "Cover Type": "No cover type",
    "Score": 0.0,
    "Genres": "No genres"
})

# # Thay thế giá trị null trong cột Date bằng ngày hiện tại
df = df.withColumn("Date",
                   F.when(F.col("Date").isNull(), F.current_date()).otherwise(F.col("Date")))

# # Định dạng clean_date thành dạng "Published July 15, 2022" nếu null
df = df.withColumn("cleaned_date",
                   F.when(F.col("cleaned_date").isNull(),
                          F.concat(F.lit("Published "),
                                   F.date_format(F.current_date(), "MMMM dd, yyyy")))
                   .otherwise(F.col("cleaned_date")))

# Kiểm tra lại số lượng giá trị null sau khi điền giá trị
null_counts_after = df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts_after.show(truncate=False)

# Tạo bảng Books

books_df = df.select("Title", "Pages", "Cover Type", "publish_date", "Genres", "Rating", "Author", "Description") \
    .withColumnRenamed("Title", "book_title") \
    .withColumnRenamed("Pages", "num_pages") \
    .withColumnRenamed("Cover Type", "cover_type") \
    .withColumnRenamed("Description", "description") \
    .withColumn("book_id", monotonically_increasing_id()) \
    .select("book_id", "book_title", "num_pages", "cover_type", "publish_date", "Genres", "Rating", "Author", "description") \
    .withColumn("description", regexp_replace(col("description"), '"', '')) \
    .withColumn("description", regexp_replace(col("description"), '[^a-zA-Z0-9\\s]', '')) \
    .withColumn("description", F.when(col("description").isNull() | (F.trim(col("description")) == ""), "No description available")
                .when(col("description").rlike("^[0-9]+$"), "No description available")
                .when(col("description").rlike("^[\\s]+$"), "No description available")
                .when(col("description").rlike("^[0-9]+[a-zA-Z]+|[a-zA-Z]+[0-9]+$"), "No description available")
                .otherwise(col("description"))) \
    .dropDuplicates()

#  Xóa dấu nháy kép và ký tự không hợp lệ trong Description
books_df = books_df.withColumn("description", regexp_replace(
    col("description"), '"', ''))  # Xóa dấu nháy kép
books_df = books_df.withColumn("description", regexp_replace(
    col("description"), '[^a-zA-Z0-9\\s]', ''))  # Xóa ký tự không hợp lệ

#  Xử lý trường hợp Description không hợp lệ
books_df = books_df.withColumn("description",
                               F.when(col("description").isNull() | (F.trim(col(
                                   "description")) == ""), "No description available")  # Thay thế NaN và chuỗi rỗng
                               # Thay thế chỉ số
                               .when(col("description").rlike("^[0-9]+$"), "No description available")
                               # Thay thế chuỗi chỉ có khoảng trắng
                               .when(col("description").rlike("^[\\s]+$"), "No description available")
                               # Thay thế chuỗi không hợp lệ
                               .when(col("description").rlike("^[0-9]+[a-zA-Z]+|[a-zA-Z]+[0-9]+$"), "No description available")
                               .otherwise(col("description")))


# Xóa dữ liệu trùng lặp
books_df = books_df.dropDuplicates()


#  Bảng authors
authors_df = df.select("Author").distinct() \
    .withColumnRenamed("Author", "author_name") \
    .withColumn("author_id", monotonically_increasing_id()) \
    .select("author_id", "author_name")

# Bảng genres
genres_df = df.select(explode(split(col("Genres"), ",")).alias("genre_name")).distinct() \
    .withColumn("genre_id", monotonically_increasing_id()) \
    .select("genre_id", "genre_name")


# Tạo bảng book_genres từ books_df và genres_df
book_genres_df = books_df \
    .select("book_id", explode(split(col("Genres"), ",")).alias("genre_name")) \
    .join(genres_df, "genre_name", "inner") \
    .select("book_id", "genre_id") \
    .dropDuplicates()


# Bảng ratings

ratings_df = df.select("Title", "Rating", "Number of Ratings", "Reviews", "Score") \
    .withColumnRenamed("Title", "book_title") \
    .withColumnRenamed("Rating", "rating") \
    .withColumnRenamed("Number of Ratings", "num_ratings") \
    .withColumnRenamed("Reviews", "num_reviews") \
    .withColumnRenamed("Score", "score") \
    .withColumn("rating_id", monotonically_increasing_id()) \
    .select("rating_id", "book_title", "rating", "num_ratings", "num_reviews", "score")


#  Bảng reviews


reviews_df = (
    df.select("Title", "Reviews", "Score")
      .withColumnRenamed("Title", "book_title")
      .join(books_df, "book_title", "inner")
      .withColumn("review_id", monotonically_increasing_id())
      .select("review_id", "book_id", "Reviews", "Score")
)


# Giả sử books_df và ratings_df đã được chuẩn bị và có cột book_title
books_with_ratings = books_df \
    .join(ratings_df, books_df.book_title == ratings_df.book_title, "left") \
    .select(
        books_df["*"],
        ratings_df["rating_id"],
        ratings_df["rating"],
        ratings_df["num_ratings"],
        ratings_df["num_reviews"],
        ratings_df["score"]
    )


# Nối ba bảng: books, genres, và book_genres
final_df = books_df \
    .join(book_genres_df, "book_id", "inner") \
    .join(genres_df, "genre_id", "inner") \
    .select("book_id", "book_title", "num_pages", "cover_type", "publish_date",
            "Rating", "Author", "description", "genre_name")

# Tạo khóa chính cho bảng books_df (giả định rằng chưa có book_id)

books_df = books_df.withColumn("book_id", monotonically_increasing_id())
ratings_df = ratings_df.select("book_title", "rating", "num_ratings", "num_reviews", "score") \
    .withColumn("rating_id", monotonically_increasing_id()) \
    .select("rating_id", "book_title", "rating", "num_ratings", "num_reviews", "score")
books_ratings_df = books_df \
    .join(ratings_df, books_df.book_title == ratings_df.book_title, "inner") \
    .select(books_df["book_id"], ratings_df["rating_id"])
books_ratings_df.show(truncate=False)

# Nối bảng books_ratings_df với books_df qua book_id
books_ratings_with_details_df = books_ratings_df \
    .join(books_df, "book_id", "inner") \
    .select(books_ratings_df["book_id"],
            books_df["book_title"],
            books_df["num_pages"],
            books_df["cover_type"],
            books_df["publish_date"],
            books_df["Genres"],
            books_df["Rating"],
            books_df["Author"],
            books_df["description"],
            books_ratings_df["rating_id"])


# Tạo cột book_id cho bảng df thông qua book_title
reviews_df = (
    df.select("Title", "Reviews", "Score")
      .withColumnRenamed("Title", "book_title")  # Đổi tên cột cho phù hợp
    # Nối với books_df qua cột book_title
      .join(books_df, "book_title", "inner")
    # Tạo review_id duy nhất
      .withColumn("review_id", monotonically_increasing_id())
    # Chọn các cột cần thiết
      .select("review_id", "book_id", "Reviews", "Score")
)

# Kết nối bảng books với authors theo tên tác giả (author_name)
books_with_authors = books_df.join(authors_df, books_df.Author == authors_df.author_name, "left") \
    .select(books_df["*"], authors_df["author_id"])

# Đổi tên cột Genres thành book_genres
books_with_authors = books_with_authors.withColumnRenamed(
    "Genres", "book_genres")

# Tách cột book_genres thành các giá trị riêng biệt và nối với bảng genres
books_with_authors = books_with_authors \
    .withColumn("book_genres", F.explode(F.split(F.col("book_genres"), ","))) \
    .join(genres_df, F.col("book_genres") == genres_df.genre_name, "left") \
    .select(
        "book_id",
        "book_title",
        "author_id",
        "num_pages",
        "cover_type",
        "publish_date",
        "description",
        genres_df["genre_id"]
    )

# Nhóm lại theo book_id, book_title, author_id và các cột khác
books_final = books_with_authors \
    .groupBy("book_id", "book_title", "author_id", "num_pages", "cover_type", "publish_date", "description") \
    .agg(F.collect_list("genre_id").alias("genre_ids"))

# Định dạng lại cột publish_date theo kiểu dd-MM-yyyy
books_final = books_final.withColumn(
    "publish_date", F.date_format(F.col("publish_date"), "dd-MM-yyyy")
)

# Chuẩn bị bảng ratings_df với các cột cần thiết và tạo rating_id
ratings_df = df.select("Title", "Rating", "Number of Ratings", "Reviews", "Score") \
    .withColumnRenamed("Title", "book_title") \
    .withColumnRenamed("Rating", "rating") \
    .withColumnRenamed("Number of Ratings", "num_ratings") \
    .withColumnRenamed("Reviews", "num_reviews") \
    .withColumnRenamed("Score", "score") \
    .withColumn("rating_id", monotonically_increasing_id())

# Nối bảng ratings_df với books_df để lấy book_id và tạo bảng books_with_ratings
books_with_ratings = ratings_df.join(books_df, ratings_df.book_title == books_df.book_title, "left") \
    .select(
        books_df["*"],
        ratings_df["rating_id"],
        ratings_df["rating"],
        ratings_df["num_ratings"],
        ratings_df["num_reviews"],
        ratings_df["score"]
)

# Hiển thị bảng books_with_ratings sau khi nối
books_with_ratings.show(truncate=False)

# Thực hiện phép nối giữa books_df và ratings_df dựa trên cột book_title
books_with_ratings_df = books_df.join(
    ratings_df,
    books_df.book_title == ratings_df.book_title,
    how="left"  # Sử dụng "left" join để giữ lại tất cả các sách, ngay cả khi không có đánh giá
)

# Thực hiện phép nối giữa books_df và ratings_df dựa trên cột book_title để tạo bảng trung gian
book_ratings_df = books_df.join(
    ratings_df,
    books_df.book_title == ratings_df.book_title,
    how="inner"
).select(
    books_df["book_id"],
    ratings_df["rating_id"]
)

# Nối bảng books_df với reviews_df qua cột book_id
books_reviews_df = books_df.join(reviews_df, "book_id", "inner")

# Thống kê số lượng sách theo tác giả
author_book_counts = books_final.groupBy("author_id").count() \
    .join(authors_df, "author_id") \
    .select("author_name", "count") \
    .orderBy(col("count").desc())


# Thống kê số lượng sách theo thể loại

genre_book_counts = books_final \
    .withColumn("genre_id", F.explode("genre_ids")) \
    .groupBy("genre_id") \
    .count() \
    .join(genres_df, "genre_id") \
    .select("genre_name", "count") \
    .orderBy(F.col("count").desc())

# Tính điểm trung bình và số lượng đánh giá cho từng cuốn sách
book_ratings = ratings_df.groupBy("book_title") \
    .agg({
        "rating": "avg",
        "num_ratings": "sum",
        "num_reviews": "sum"
    }) \
    .withColumnRenamed("avg(rating)", "average_rating") \
    .withColumnRenamed("sum(num_ratings)", "total_ratings") \
    .withColumnRenamed("sum(num_reviews)", "total_reviews") \
    .orderBy(col("average_rating").desc())

# Tính số lượng giá trị null trong mỗi cột
null_counts = df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show(truncate=False)

books_final = books_final.withColumn(
    "publish_date", F.to_date("publish_date", "dd-MM-yyyy")
)

# Ghi dữ liệu vào thư mục tạm thời
authors_df.coalesce(1).write.format("csv").option("header", "true").save(
    "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\authors_temp")

# Lấy file phân mảnh và di chuyển vào tên file mong muốn
temp_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\authors_temp"
destination_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\authors.csv"

# Tìm file .csv duy nhất và di chuyển
for file in os.listdir(temp_path):
    if file.startswith("part-"):
        shutil.move(os.path.join(temp_path, file), destination_path)
        break

# Xóa thư mục tạm thời
shutil.rmtree(temp_path)

print(f"Dữ liệu đã được lưu vào {destination_path}")
# Chuyển dữ liệu từ Spark DataFrame sang Pandas DataFrame
authors_pandas_df = authors_df.toPandas()

# Lưu dữ liệu vào CSV
authors_pandas_df.to_csv(
    "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\authors.csv", index=False)

print("Dữ liệu đã được lưu vào authors.csv")


# Chuyển cột 'genre_ids' từ kiểu ARRAY thành chuỗi (string)
books_final = books_final.withColumn(
    "genre_ids", F.concat_ws(",", F.col("genre_ids"))
)

# Ghi dữ liệu vào thư mục tạm thời (coalesce(1) để chỉ xuất ra 1 file)
books_final.coalesce(1).write.format("csv").option("header", "true").save(
    "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\books_temp")

# Lấy file phân mảnh và di chuyển vào tên file mong muốn
temp_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\books_temp"
destination_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\books.csv"

# Tìm file .csv duy nhất và di chuyển
for file in os.listdir(temp_path):
    if file.startswith("part-"):
        shutil.move(os.path.join(temp_path, file), destination_path)
        break

# Xóa thư mục tạm thời
shutil.rmtree(temp_path)

# Thông báo đã lưu dữ liệu vào CSV
print(f"Dữ liệu đã được lưu vào {destination_path}")

# Đường dẫn thư mục tạm thời và đích
temp_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\ratings_temp"
destination_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\ratings.csv"

# Ghi dữ liệu vào thư mục tạm thời cho ratings_df
ratings_df.coalesce(1).write.format("csv").option(
    "header", "true").save(temp_path)

# Kiểm tra và di chuyển file .csv duy nhất từ thư mục tạm thời vào thư mục đích
file_moved = False
for file in os.listdir(temp_path):
    if file.startswith("part-"):
        shutil.move(os.path.join(temp_path, file), destination_path)
        file_moved = True
        print(f"File đã được di chuyển vào {destination_path}")
        break

if not file_moved:
    print("Không tìm thấy file .csv để di chuyển!")

# Xóa thư mục tạm thời sau khi di chuyển file
shutil.rmtree(temp_path)
print(f"Đã xóa thư mục tạm thời: {temp_path}")

# Chuyển dữ liệu từ Spark DataFrame sang Pandas DataFrame (nếu cần)
ratings_pandas_df = ratings_df.toPandas()

# Lưu dữ liệu vào CSV bằng Pandas
ratings_pandas_df.to_csv(destination_path, index=False)
print("Dữ liệu đã được lưu vào ratings.csv")
spark = SparkSession.builder.appName("GenresProcessing").getOrCreate()
genres_df.coalesce(1).write.format("csv").option("header", "true").save(
    "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\genres_temp")
temp_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\genres_temp"
destination_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\genres.csv"

for file in os.listdir(temp_path):
    if file.startswith("part-"):
        shutil.move(os.path.join(temp_path, file), destination_path)
        break

shutil.rmtree(temp_path)

print(f"Dữ liệu đã được lưu vào {destination_path}")

# Lưu reviews_df thành file CSV
reviews_df.coalesce(1).write.format("csv").option("header", "true") \
    .save("D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\reviews_temp")

# Di chuyển file CSV từ thư mục tạm thời
temp_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\reviews_temp"
destination_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\reviews.csv"

# Tìm file CSV duy nhất và di chuyển
for file in os.listdir(temp_path):
    if file.startswith("part-"):
        shutil.move(os.path.join(temp_path, file), destination_path)
        break

# Xóa thư mục tạm thời
shutil.rmtree(temp_path)

print(f"Dữ liệu đã được lưu vào {destination_path}")

# Tạo bảng book_genres từ books_df và genres_df
book_genres_df = books_df \
    .select("book_id", explode(split(col("Genres"), ",")).alias("genre_name")) \
    .join(genres_df, "genre_name", "inner") \
    .select("book_id", "genre_id") \
    .dropDuplicates()


# Lưu book_genres_df thành file CSV
book_genres_df.coalesce(1).write.format("csv").option("header", "true") \
    .save("D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\book_genres_temp")

# Di chuyển file CSV từ thư mục tạm thời
temp_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\book_genres_temp"
destination_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\book_genres.csv"

# Tìm file CSV duy nhất và di chuyển
for file in os.listdir(temp_path):
    if file.startswith("part-"):
        shutil.move(os.path.join(temp_path, file), destination_path)
        break

# Xóa thư mục tạm thời
shutil.rmtree(temp_path)

print(f"Dữ liệu đã được lưu vào {destination_path}")

# Thực hiện phép nối giữa books_df và ratings_df dựa trên cột book_title
book_ratings_df = books_df.join(
    ratings_df,
    books_df.book_title == ratings_df.book_title,
    how="inner"
).select(
    books_df["book_id"],
    ratings_df["rating_id"]
)

# Kiểm tra tính hợp lệ của book_id trong bảng books và rating_id trong bảng ratings
# Giả sử books_df và ratings_df đều có các cột book_id và rating_id
# Giả sử bạn đã có df_books chứa danh sách book_id hợp lệ
valid_books_df = books_df.select("book_id")
# Giả sử bạn đã có df_ratings chứa danh sách rating_id hợp lệ
valid_ratings_df = ratings_df.select("rating_id")

# Lọc book_ratings_df chỉ giữ lại các bản ghi có book_id hợp lệ trong bảng books và rating_id hợp lệ trong bảng ratings
book_ratings_df = book_ratings_df.join(
    valid_books_df, on="book_id", how="inner")
book_ratings_df = book_ratings_df.join(
    valid_ratings_df, on="rating_id", how="inner")

# Kiểm tra kết quả sau khi lọc
print(f"Số bản ghi hợp lệ sau khi lọc: {book_ratings_df.count()}")

# Đường dẫn tạm thời và đích để lưu file CSV
temp_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\book_ratings_temp"
destination_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\book_ratings.csv"

# Kiểm tra nếu file đã tồn tại và xóa nó
if os.path.exists(destination_path):
    os.remove(destination_path)
    print(f"Đã xóa file cũ tại {destination_path}")

# Lưu book_ratings_df thành file CSV
book_ratings_df.coalesce(1).write.format("csv").option("header", "true") \
    .save(temp_path)

# Di chuyển file CSV từ thư mục tạm thời
for file in os.listdir(temp_path):
    if file.startswith("part-"):
        shutil.move(os.path.join(temp_path, file), destination_path)
        break

# Xóa thư mục tạm thời
shutil.rmtree(temp_path)

print(f"Dữ liệu hợp lệ đã được lưu vào {destination_path}")

# Đọc file CSV vào DataFrame
csv_file_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\authors.csv"
try:
    df = pd.read_csv(csv_file_path)
    print("Đọc dữ liệu từ file CSV thành công.")
except FileNotFoundError:
    print(f"Không tìm thấy file tại {csv_file_path}.")
    exit()

# Tạo chuỗi kết nối tới SQL Server
connection_string = "mssql+pyodbc://GNAHT41\\SQLEXPRESS/goodreads_books?driver=ODBC+Driver+17+for+SQL+Server"

# Kết nối tới SQL Server
try:
    engine = sqlalchemy.create_engine(connection_string)
    with engine.connect() as connection:
        print("Kết nối tới SQL Server thành công.")
except SQLAlchemyError as e:
    print(f"Lỗi khi kết nối tới SQL Server: {e}")
    exit()

# Xóa dữ liệu cũ và chèn dữ liệu mới
try:
    with engine.connect() as connection:
        # Tạm thời xóa ràng buộc khóa ngoại
        connection.execute(
            "ALTER TABLE books NOCHECK CONSTRAINT FK_books_authors1")
        print("Ràng buộc khóa ngoại đã bị tắt.")

        # Xóa toàn bộ dữ liệu trong bảng authors
        connection.execute("DELETE FROM authors")
        print("Dữ liệu cũ trong bảng 'authors' đã được xóa.")

        # Chèn dữ liệu mới từ file CSV
        df.to_sql('authors', con=engine, if_exists='append', index=False)
        print("Dữ liệu mới đã được chèn vào bảng 'authors'.")

        # Khôi phục lại ràng buộc khóa ngoại
        connection.execute(
            "ALTER TABLE books CHECK CONSTRAINT FK_books_authors1")
        print("Ràng buộc khóa ngoại đã được khôi phục.")
except SQLAlchemyError as e:
    print(f"Lỗi khi xóa và chèn dữ liệu vào SQL Server: {e}")


# Đọc file CSV vào DataFrame
csv_file_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\books.csv"
# Dùng 'on_bad_lines' để bỏ qua các dòng lỗi nếu có
df = pd.read_csv(csv_file_path, on_bad_lines='skip')

# Loại bỏ cột 'book_id' nếu nó là identity column
df = df.drop(columns=['book_id'])

# Chuyển cột publish_date thành kiểu datetime
df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')
df['publish_date'] = df['publish_date'].dt.date  # Chuyển từ datetime sang date


# Kết nối tới SQL Server bằng SQLAlchemy
connection_string = "mssql+pyodbc://sa:thangvt4102004@GNAHT41\\SQLEXPRESS/goodreads_books?driver=ODBC+Driver+17+for+SQL+Server"

engine = sqlalchemy.create_engine(connection_string)
try:
    with engine.connect() as conn:
        print("Kết nối đến SQL Server thành công!")
except Exception as e:
    print(f"Đã có lỗi khi kết nối: {e}")

# Chèn dữ liệu vào bảng SQL (bảng 'books')
try:
    df.to_sql('books', con=engine, if_exists='append', index=False)
    print("Dữ liệu đã được đưa vào bảng 'books' trong SQL Server.")
except Exception as e:
    print(f"Đã có lỗi khi chèn dữ liệu: {e}")

# Đọc file CSV vào DataFrame
csv_file_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\ratings.csv"
df = pd.read_csv(csv_file_path, on_bad_lines='skip')

# Loại bỏ các dòng trùng lặp dựa trên 'rating_id'
df = df.drop_duplicates(subset=['rating_id'])

# Kết nối tới SQL Server bằng SQLAlchemy
connection_string = "mssql+pyodbc://sa:thangvt4102004@GNAHT41\\SQLEXPRESS/goodreads_books?driver=ODBC+Driver+17+for+SQL+Server"

engine = sqlalchemy.create_engine(connection_string)

# Lọc bỏ các giá trị đã tồn tại trong SQL
try:
    with engine.connect() as conn:
        existing_ids = pd.read_sql("SELECT rating_id FROM ratings", con=conn)
        df = df[~df['rating_id'].isin(existing_ids['rating_id'])]
except Exception as e:
    print(f"Lỗi khi lấy dữ liệu từ SQL Server: {e}")

# Chèn dữ liệu vào SQL Server
try:
    df.to_sql('ratings', con=engine, if_exists='append', index=False)
    print("Dữ liệu đã được đưa vào bảng 'ratings' trong SQL Server.")
except Exception as e:
    print(f"Đã có lỗi khi chèn dữ liệu: {e}")

# Đọc file CSV vào DataFrame
csv_file_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\reviews.csv"
df_reviews = pd.read_csv(csv_file_path)

# Kết nối tới SQL Server bằng SQLAlchemy
connection_string = "mssql+pyodbc://sa:thangvt4102004@GNAHT41\\SQLEXPRESS/goodreads_books?driver=ODBC+Driver+17+for+SQL+Server"

engine = sqlalchemy.create_engine(connection_string)

# Lấy dữ liệu từ bảng 'books' để kiểm tra book_id
try:
    with engine.connect() as conn:
        # Lấy tất cả book_id từ bảng 'books'
        books_df = pd.read_sql("SELECT book_id FROM books", con=conn)
        print("Đã lấy dữ liệu từ bảng 'books'.")
except Exception as e:
    print(f"Đã có lỗi khi lấy dữ liệu từ bảng 'books': {e}")

# Lọc các bản ghi có book_id hợp lệ (tồn tại trong bảng 'books')
df_reviews = df_reviews[df_reviews['book_id'].isin(books_df['book_id'])]

# Bỏ cột review_id
df_reviews.drop(columns=['review_id'], inplace=True)

# Chèn dữ liệu vào bảng 'reviews'
try:
    df_reviews.to_sql('reviews', con=engine, if_exists='append', index=False)
    print("Dữ liệu đã được đưa vào bảng 'reviews' trong SQL Server.")
except Exception as e:
    print(f"Đã có lỗi khi chèn dữ liệu: {e}")

# Đọc file CSV vào DataFrame
csv_file_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\reviews.csv"
df_reviews = pd.read_csv(csv_file_path)

# Kiểm tra kiểu dữ liệu của các cột trong DataFrame
print(df_reviews.dtypes)

# Đổi kiểu dữ liệu nếu cần
df_reviews['review_id'] = df_reviews['review_id'].astype('int64')  # bigInt
df_reviews['book_id'] = df_reviews['book_id'].astype('int64')      # bigInt
df_reviews['Reviews'] = df_reviews['Reviews'].astype('int')        # int
df_reviews['Score'] = df_reviews['Score'].astype('int')            # int

# Kết nối tới SQL Server bằng SQLAlchemy
connection_string = "mssql+pyodbc://sa:thangvt4102004@GNAHT41\\SQLEXPRESS/goodreads_books?driver=ODBC+Driver+17+for+SQL+Server"

engine = sqlalchemy.create_engine(connection_string)

# Lấy dữ liệu book_id từ bảng books
try:
    with engine.connect() as conn:
        # Lấy tất cả book_id từ bảng 'books'
        books_df = pd.read_sql("SELECT book_id FROM books", con=conn)
        print("Đã lấy dữ liệu từ bảng 'books'.")
except Exception as e:
    print(f"Đã có lỗi khi lấy dữ liệu từ bảng 'books': {e}")

# Lọc các bản ghi có book_id hợp lệ (tồn tại trong bảng 'books')
df_reviews = df_reviews[df_reviews['book_id'].isin(books_df['book_id'])]

# Bỏ cột review_id
df_reviews.drop(columns=['review_id'], inplace=True)

# Chèn dữ liệu vào bảng reviews
try:
    df_reviews.to_sql('reviews', con=engine, if_exists='append', index=False)
    print("Dữ liệu đã được đưa vào bảng 'reviews' trong SQL Server.")
except Exception as e:
    print(f"Đã có lỗi khi chèn dữ liệu: {e}")

# Đọc file CSV vào DataFrame
csv_file_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\book_genres.csv"
df_book_genres = pd.read_csv(csv_file_path)

# Kết nối tới SQL Server bằng SQLAlchemy
connection_string = "mssql+pyodbc://sa:thangvt4102004@GNAHT41\\SQLEXPRESS/goodreads_books?driver=ODBC+Driver+17+for+SQL+Server"

engine = sqlalchemy.create_engine(connection_string)

# Lấy tất cả genre_id từ bảng genres
try:
    with engine.connect() as conn:
        genres_df = pd.read_sql("SELECT genre_id FROM genres", con=conn)
        print("Đã lấy dữ liệu genre_id từ bảng 'genres'.")
except Exception as e:
    print(f"Đã có lỗi khi lấy dữ liệu từ bảng 'genres': {e}")

# Lọc các bản ghi có genre_id hợp lệ (tồn tại trong bảng 'genres')
df_book_genres = df_book_genres[df_book_genres['genre_id'].isin(
    genres_df['genre_id'])]

# Chèn dữ liệu vào bảng book_genres
try:
    df_book_genres.to_sql('book_genres', con=engine,
                          if_exists='append', index=False)
    print("Dữ liệu đã được đưa vào bảng 'book_genres' trong SQL Server.")
except Exception as e:
    print(f"Đã có lỗi khi chèn dữ liệu: {e}")

# Đọc file CSV vào DataFrame
csv_file_path = "D:\\My Projects\\crawl-data-ptdl\\spark_with_sql\\save_table_sql\\book_ratings.csv"
df_books_ratings = pd.read_csv(csv_file_path)

# Loại bỏ cột rating_id nếu có
df_books_ratings = df_books_ratings.drop(
    columns=["rating_id"], errors='ignore')

# Tạo cột rating_id tự động tăng nếu cần
df_books_ratings['rating_id'] = range(1, len(df_books_ratings) + 1)

# Kết nối tới SQL Server bằng SQLAlchemy
connection_string = "mssql+pyodbc://sa:thangvt4102004@GNAHT41\\SQLEXPRESS/goodreads_books?driver=ODBC+Driver+17+for+SQL+Server"

engine = sqlalchemy.create_engine(connection_string)

# Lấy tất cả rating_id có sẵn trong bảng ratings
with engine.connect() as connection:
    available_rating_ids = pd.read_sql(
        "SELECT rating_id FROM ratings", con=connection)

# Lọc df_books_ratings chỉ giữ lại các rating_id có trong bảng ratings
df_books_ratings = df_books_ratings[df_books_ratings['rating_id'].isin(
    available_rating_ids['rating_id'])]

# Lấy tất cả book_id có sẵn trong bảng books
with engine.connect() as connection:
    available_book_ids = pd.read_sql(
        "SELECT book_id FROM books", con=connection)

# Lọc df_books_ratings chỉ giữ lại các book_id có trong bảng books
df_books_ratings = df_books_ratings[df_books_ratings['book_id'].isin(
    available_book_ids['book_id'])]


# Xóa tất cả dữ liệu trong bảng book_ratings trước khi chèn dữ liệu mới
try:
    with engine.connect() as connection:
        connection.execute("DELETE FROM book_ratings")
    print("Đã xóa dữ liệu cũ trong bảng 'book_ratings'.")

    # Chèn dữ liệu vào bảng book_ratings và ghi đè dữ liệu cũ
    df_books_ratings.to_sql('book_ratings', con=engine,
                            if_exists='append', index=False)
    print("Dữ liệu mới đã được đưa vào bảng 'book_ratings' trong SQL Server.")

except sqlalchemy.exc.IntegrityError as ie:
    # Xử lý lỗi IntegrityError (ví dụ: vi phạm ràng buộc khóa ngoại, NULL không hợp lệ)
    print(f"Lỗi ràng buộc (IntegrityError): {ie.orig}")
except sqlalchemy.exc.OperationalError as oe:
    # Xử lý lỗi khi kết nối hoặc thao tác cơ sở dữ liệu
    print(f"Lỗi kết nối cơ sở dữ liệu (OperationalError): {oe.orig}")
except Exception as e:
    # Xử lý các lỗi khác
    print(f"Đã có lỗi khi chèn dữ liệu: {e}")
