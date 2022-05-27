# direct substitutions
s/===/==/g
s/=!=/!=/g
s/\/\//#/g
s/null/None/g
s/not/~/g
s/\->/:/g
s/true/True/g
s/false/False/g
s/isInCollection/isin/g

# column invocation standardization to col()
s/[\$]([^ ]*)/col(\1)/g
s/[\']([a-zA-Z0-9]*)/col(\"\1\")/g

# prepend F. to all functions and T.<>() to type invocations
s/(abs|acos|add_months|approxCountDistinct|approx_count_distinct|array|array_contains|array_distinct|array_except|array_intersect|array_join|array_max|array_min|array_position|array_remove|array_repeat|array_sort|array_union|arrays_overlap|arrays_zip|asc|asc_nulls_first|asc_nulls_last|ascii|asin|atan|atan2|avg|base64|basestring|bin|bitwiseNOT|blacklist|broadcast|bround|cbrt|ceil|coalesce|col|collect_list|collect_set|column|concat|concat_ws|conv|corr|cos|cosh|count|countDistinct|covar_pop|covar_samp|crc32|create_map|cume_dist|current_date|current_timestamp|date_add|date_format|date_sub|date_trunc|datediff|dayofmonth|dayofweek|dayofyear|decode|degrees|dense_rank|desc|desc_nulls_first|desc_nulls_last|element_at|encode|exp|explode|explode_outer|expm1|expr|factorial|first|flatten|floor|format_number|format_string|from_json|from_unixtime|from_utc_timestamp|functools|get_json_object|greatest|grouping|grouping_id|hash|hex|hour|hypot|ignore_unicode_prefix|initcap|input_file_name|instr|isnan|isnull|json_tuple|kurtosis|lag|last|last_day|lead|least|length|levenshtein|lit|locate|log|log10|log1p|log2|lower|lpad|ltrim|map_concat|map_from_arrays|map_from_entries|map_keys|map_values|max|md5|mean|min|minute|monotonically_increasing_id|month|months_between|nanvl|next_day|ntile|pandas_udf|percent_rank|posexplode|posexplode_outer|pow|quarter|radians|rand|randn|rank|regexp_extract|regexp_replace|repeat|reverse|rint|round|row_number|rpad|rtrim|schema_of_json|second|sequence|sha1|sha2|shiftLeft|shiftRight|shiftRightUnsigned|shuffle|signum|sin|since|sinh|size|skewness|slice|sort_array|soundex|spark_partition_id|split|sqrt|stddev|stddev_pop|stddev_samp|struct|substring|substring_index|sum|sumDistinct|sys|tan|tanh|toDegrees|toRadians|to_date|to_json|to_timestamp|to_utc_timestamp|translate|trim|trunc|udf|unbase64|unhex|unix_timestamp|upper|var_pop|var_samp|variance|warnings|weekofyear|when|window|year)\(/F.\1(/g
s/(ByteType|ShortType|IntegerType|LongType|FloatType|DoubleType|DecimalType|StringType|BinaryType|BooleanType|TimestampType|DateType|ArrayType|MapType|StructType|StructField)/T.\1()/g

# val var removal
s/(val|var)[ ]+//g

# add parentheses to specific col functions
s/\.desc/.desc()/g
s/\.asc/.asc()/g
s/((\bisNull\b)|(\bisNotNull\b))([\)| |,|\n])/\1()\4/g

# add backslash to end of line
s/([a-zA-Z0-9\-_]*) = ([a-zA-Z0-9\-_]*)$/\1 = \2\\/g
s/(\))($)/\1\\/g

# substitute conditional operators and prepare condition separation with parentheses
s/ \&\& /\) \& (/g
s/ \|\| /\) | \(/g

# add parentheses to composed conditions
s/(\bwhere\b)\( *(.*(\&|\|).*) *[\)\\]/\1((\2)\\/g
s/(\bwhen\b)\( *(.*(\&|\|).*) *,/\1((\2),/g

# fix na.drop operation
s/na\.drop\((.*)\)/na.drop(subset=\1)/g
s/na.drop(subset=)/na.drop()/g

# swap na.replace args
s/na\.replace\([ |\n]*(.*)[ |\n]*,[ |\n]*(.*)[ |\n]*\)/na.replace(\2, \1)/g

# fix specific join calls
s/(.join\(.*== )((.*)\("(.*)"\)),/\1\3.\4,/g

# add new imports
1i from pyspark.sql import SparkSession
1i from pyspark.sql import functions as F
1i from pyspark.sql import types as T
1i from pyspark.sql import Window

# remove old imports
/import org\.apache.spark.*/d
/import spark.*/d

# fix SparkSession
s/SparkSession/SparkSession.builder/g
/\.builder\(\)/d
