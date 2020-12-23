library(tidyverse)

dir_root = "/mnt"
dir_mount = "diabetes"
dir_fullpath = file.path(dir_root, dir_mount)
dir_fullpath_file = file.path(dir_fullpath, "diabetes.csv")

df_diabetes = read.csv(dir_fullpath_file)

target = "Outcome"
df_diabetes_mean_features = as.data.frame.list(colMeans(df_diabetes)) %>% select(-Outcome)

df_diabetes$Outcome = as.factor(df_diabetes$Outcome)

model <- glm(Outcome ~ ., data=df_diabetes, family=binomial)

#predict(model, newdata=df_diabetes_mean_features, type="response")

saveRDS(model, file.path(dir_fullpath, "model.rds"))
write.csv(df_diabetes_mean_features, file.path(dir_fullpath, "diabetes_mean.csv"), row.names=FALSE)
