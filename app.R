library(tidyverse)

dir_root = "/mnt"
dir_mount = "diabetes"
dir_fullpath = file.path(dir_root, dir_mount)
dir_fullpath_file = file.path(dir_fullpath, "diabetes_mean.csv")

mount_path <- dir_root
df_diabetes_mean = read.csv(dir_fullpath_file)
model = readRDS(file.path(dir_fullpath, "model.rds"))
f <-file(file.path(dir_fullpath, paste("app-", Sys.getenv("DOMINO_RUN_ID"),".log",sep="")))

predict_model <- function(glucose, blood_pressure, bmi) {
  df_diabetes_score <- df_diabetes_mean
  df_diabetes_score$Glucose <- as.numeric(glucose)
  df_diabetes_score$BloodPressure <- as.numeric(blood_pressure)
  df_diabetes_score$BMI <- as.numeric(bmi)
  prediction <- as.numeric(predict(model, newdata=df_diabetes_score, type="response"))
  return(prediction)
}

server <- function(input, output) {
  output$mount_path <- renderText({
    paste(system2("ls", mount_path, stdout=TRUE, stderr=TRUE), collapse=", ")
  })

  output$prediction <- renderText(predict_model(input$glucose, input$blood_pressure, input$bmi))
}

ui <- fluidPage(
  sidebarLayout(
    sidebarPanel(
      textInput("glucose", "Glucose", 120),
      textInput("blood_pressure", "Blood Pressure", 70),
      textInput("bmi", "BMI", 32)
    ),

    mainPanel(
      h2("Diabetes Probability"),
      textOutput("prediction"),
      br(),
      br(),
      h2("Files in Mount Path"),
      strong(paste("These are the contents at:", mount_path, sep=" ")),
      br(),
      br(),
      textOutput("mount_path")
    )
  )
)

shinyApp(ui = ui, server = server)
