# Databricks notebook source
# MAGIC %md
# MAGIC # It's the end of the world!
# MAGIC We are testing how much data a map in Shiny can handle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

library(shiny)
library(datasets)
library(tidyverse)
library(leaflet)
library(ggplot2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating dataset

# COMMAND ----------

## Setting number of observations in the dataset -----------------------------
n_obs <- 100000

## First we create and populate a dataframe -----------------------------
df <- data.frame(id = c(1:n_obs),
                 lat = round(runif(n_obs, -90, 90), 2),
                 long = round(runif(n_obs, -180, 180), 2),
                 depth = sample(0:700, n_obs,  replace = TRUE),
                 mag = round(runif(n_obs, 0.1, 9.6), 1),
                 stations = sample(0:100, n_obs,  replace = TRUE),
                 time = as.Date("2000-01-01")
                 + sample.int(as.Date("2019-12-31") - as.Date("2010-01-01"),
                              n_obs, replace = TRUE)
                 )

## And then passing it to the DF we are going to use in Shiny ------------------
my_data <- df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shiny app

# COMMAND ----------

## This section creates UI for the app -----------------------------
ui <- fluidPage(
  titlePanel("Interactive map for earthquakes"),  ## Header for UI

  sidebarLayout(position = "right",  ## Putting the sidebar on the right side

                ## This is the section for filter and summary statistics
                sidebarPanel(
## Name of input variables for the map based on the filter we are creating
                  sliderInput("timeRange",
                              label = "Time of interest:",
                              min(my_data$time),
                              max(my_data$time),
                              value = range(my_data$time)
                             ),
                  textOutput("mean_mag"),  ## Summary statistics 1
                  textOutput("med_stations") ## Summary statistics 2
                ),

## This section is for the map and values related to a specific point on the map
                mainPanel(
                  leafletOutput("map",  height = 600),  ## Interactive map
                  tableOutput("my_table"), ## Datapoints for a specific location
                  plotOutput("my_plot") ## Plot for a specific location
                )
  )
)
## This section creates all the data we are using in the UI --------------------
server <- shinyServer(function(input,  output) {

  # Enabling "click values"
  data <- reactiveValues(clicked_marker = NULL)

  ### Output for side panel

  # Function for printing mean for magnitude
  output$mean_mag <- renderText({
    paste("Average mag for range is:",
         my_data %>%
           ## Filtering dataframe based on slider values in UI
               filter(time >= input$timeRange[1],
                      time <= input$timeRange[2]) %>%
               summarise(mean(mag))) # Printing value
  })

  # Function for printing median stations observing an earthquake
  output$med_stations <- renderText({
    paste("Median stations for range is:",
         my_data %>%
           ## Filtering dataframe based on slider values in UI
               filter(time >= input$timeRange[1],
                      time <= input$timeRange[2]) %>%
               summarise(median(stations))) # Printing value
  })

  ### Output for main panel

  # Function for displaying interactive map
  output$map <- renderLeaflet({
    my_data %>%
      ## Filtering dataframe based on slider values in UI
          filter(time >= input$timeRange[1],  time <= input$timeRange[2]) %>%
          leaflet() %>%
          addTiles() %>%
          addMarkers(~long,  ~lat,  layerId = ~id,
                     clusterOptions = markerClusterOptions())
  })

  # Functions related to click on map
  observeEvent(input$map_marker_click, {
    data$clicked_marker <- input$map_marker_click

    # Function for printing a table with values for observation
    output$my_table <- renderTable({
      return(
            subset(my_data,  id == data$clicked_marker$id)
      )
    })

# Function for plotting a value and compare it to average for the entire dataset
    output$my_plot <- renderPlot({
      return(
            ggplot(data = subset(my_data, id == data$clicked_marker$id),
                   aes(mag, 1)) +
                  geom_point() +
                  geom_vline(xintercept = mean(df$mag),  color = "red",
                             linetype = "dotted") +
                    xlim(0, 10) +
                    theme(axis.title.y = element_blank(),
                          axis.text.y = element_blank(),
                          axis.ticks.y = element_blank())
      )
    })

  })
})
shinyApp(ui,  server)

# COMMAND ----------

# MAGIC %md
# MAGIC **Kill switch for app**

# COMMAND ----------

# MAGIC %sh kill 3912