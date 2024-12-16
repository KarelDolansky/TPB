from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, DataTypes, Schema, FormatDescriptor

# Initialize Flink Table API
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# File paths
ratings_path = "u.data"
movies_path = "u-mod.item"

# Step 1: Define schema and load data
ratings_table = TableDescriptor.for_connector("filesystem") \
    .schema(Schema.new_builder()
            .column("user", DataTypes.INT())
            .column("movie", DataTypes.INT())
            .column("score", DataTypes.INT())
            .column("time", DataTypes.BIGINT())
            .build()) \
    .option("path", ratings_path) \
    .format(FormatDescriptor.for_format("csv").option("field-delimiter", "\t").build()) \
    .build()

movies_table = TableDescriptor.for_connector("filesystem") \
    .schema(Schema.new_builder()
            .column("movie_id", DataTypes.INT())
            .column("title", DataTypes.STRING())
            .build()) \
    .option("path", movies_path) \
    .format(FormatDescriptor.for_format("csv").option("field-delimiter", "|").build()) \
    .build()

table_env.create_table("ratings", ratings_table)
ratings = table_env.from_path("ratings")

table_env.create_table("movies", movies_table)
movies = table_env.from_path("movies")

# Step 2: Calculate average rating for each movie
filtered_ratings = ratings.filter(ratings.score == 5)

count_ratings = filtered_ratings.group_by(filtered_ratings.movie) \
    .select(filtered_ratings.movie.alias("movie_id_count"), 
            filtered_ratings.score.count.alias("five_star_count")
            )

average_ratings = ratings.group_by(ratings.movie) \
    .select(ratings.movie.alias("movie_id_avg"), 
            ratings.score.avg.alias("average_score")
            )

combined_ratings = count_ratings.join(average_ratings) \
    .where(count_ratings.movie_id_count == average_ratings.movie_id_avg) \
    .select(count_ratings.movie_id_count.alias("movie_id"), 
            count_ratings.five_star_count, 
            average_ratings.average_score
            )

# Step 3: Add movie titles
movies_renamed = movies.select(movies.movie_id.alias("movie_id_renamed"), movies.title)
final_table = combined_ratings.join(movies_renamed) \
    .where(combined_ratings.movie_id == movies_renamed.movie_id_renamed) \
    .select(combined_ratings.movie_id, movies_renamed.title, combined_ratings.five_star_count, combined_ratings.average_score)

sorted_table = final_table.order_by(final_table.five_star_count.desc).fetch(10)

# Step 4: Output results
output_descriptor = TableDescriptor.for_connector("print") \
    .schema(Schema.new_builder()
            .column("movie_id", DataTypes.INT())
            .column("title", DataTypes.STRING())
            .column("five_star_count", DataTypes.BIGINT())
            .column("average_score", DataTypes.DOUBLE())
            .build()) \
    .build()

table_env.create_temporary_table("output_sink", output_descriptor)
sorted_table.execute_insert("output_sink").wait()
