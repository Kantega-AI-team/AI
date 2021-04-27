# Databricks notebook source
# MAGIC %md #Adding up with more complexity in shiny <br/>
# MAGIC This time we will use two set of filters to create a subset and
# MAGIC action buttons to preform different tasks on the subset

# COMMAND ----------

# MAGIC %md #Setup

# COMMAND ----------

library(datasets)
library(shiny)
library(leaflet)
library(tidyverse)

# COMMAND ----------

# MAGIC %md #Creating a data set

# COMMAND ----------

df_state <- as.data.frame(state.x77)
df_state["Abb"] <- state.abb
df_state["Region"] <- state.region
df_state["Long"] <- state.center$x
df_state["Lat"] <- state.center$y

# COMMAND ----------

# MAGIC %md #Shiny

# COMMAND ----------

ui <- fluidPage(
  titlePanel("Interactive demo with Shiny"),
  sidebarLayout(position = "left",
                sidebarPanel(
## UI filters -----------------------------
                  h4("Filter for data set:"),
                  checkboxGroupInput("region_filter",
                                     label = "Regions:",
                                     choices = unique(df_state$Region),
                                     selected = unique(df_state$Region)),
                  br(),
                  sliderInput("pop_filter",
                              label = "Population:",
                              min(df_state$Population),
                              max(df_state$Population),
                              value = range(df_state$Population)),
                  br(),
                  h4("Different tasks"),
                  actionButton("button1", "Create a map"),
                  actionButton("button2", "Do a regression")
                ),
                mainPanel(
## UI action buttons -----------------------------
                  verbatimTextOutput("action2"),
                  leafletOutput("action1")
                )
                )
)
server <- shinyServer(function(input, output, session) {

## Subset based on UI filters -----------------------------
  filtered_data <- reactive({
    df_state %>%
      filter(Region %in% input$region_filter) %>%
      filter(Population >= input$pop_filter[1],
             Population <= input$pop_filter[2])
  })
## Linear regression for action button 2 -----------------------------
  mod <- reactive({
    lm(`Life Exp`~ Population + Income + Illiteracy + Murder + `HS Grad` + Frost
       + Area, data = filtered_data())
  })

## Adding logic to action buttons so they work -----------------------------
  but1 <- reactiveValues(but1 = FALSE)
  but2 <- reactiveValues(but2 = FALSE)

  observeEvent(input$button1,
               isolate({
                 but1$but1 = TRUE
                 but2$but2 = FALSE
               }))

  observeEvent(input$button2,
               isolate({
                 but1$but1 = FALSE
                 but2$but2 = TRUE
               }))

## Output for main panel -----------------------------
  output$action1 <- renderLeaflet({
    if (but1$but1)
      filtered_data() %>%
        leaflet() %>%
        addTiles() %>%
        addMarkers(~Long, ~Lat, clusterOptions = markerClusterOptions())
    else
      return()
  })

  output$action2 <- renderPrint({
    if (but2$but2)
      summary(mod())
    else
      return()
  })

})
shinyApp(ui, server)
