library(tidyverse)

dir_root = "/mnt"
dir_mount = "diabetes"
dir_fullpath = file.path(dir_root, dir_mount)
dir_fullpath_file = file.path(dir_fullpath, "diabetes_mean.csv")

df_diabetes_mean = read.csv(dir_fullpath_file)
model = readRDS(file.path(dir_fullpath, "model.rds"))

args <- commandArgs(trailingOnly=TRUE)
df_diabetes_mean$Glucose <- as.numeric(args[1])
df_diabetes_mean$BloodPressure <- as.numeric(args[2])
df_diabetes_mean$BMI <- as.numeric(args[3])

prediction = predict(model, newdata=df_diabetes_mean, type="response")

vec_str_log <- c(
  timestamp(),
  paste("DOMINO_STARTING_USERNAME:", Sys.getenv("DOMINO_STARTING_USERNAME"), sep=" "),
  paste("DOMINO_RUN_ID:", Sys.getenv("DOMINO_RUN_ID"), sep=" "),
  paste("\n"),
  paste("Glucose:", df_diabetes_mean$Glucose, sep=" "),
  paste("Blood Pressure:", df_diabetes_mean$BloodPressure, sep=" "),
  paste("BMI:", df_diabetes_mean$BMI, sep=" "),
  paste("\n"),
  paste(colnames(df_diabetes_mean), collapse=", "),
  paste(df_diabetes_mean[1,], collapse=", "),
  paste("\n"),
  paste("Prediction:", prediction, sep=" ")
)

f <-file(file.path(dir_fullpath, paste(Sys.getenv("DOMINO_RUN_ID"),".log",sep="")))
writeLines(vec_str_log, f)
close(f)

f <-file("output.log")
writeLines(vec_str_log, f)
close(f)
