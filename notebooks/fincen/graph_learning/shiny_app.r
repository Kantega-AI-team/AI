# Databricks notebook source
# load the required libraries

library(sparklyr)
library(dplyr)
library(tibble)
library(purrr)
library(stringr)
library(tidyr)
library(shiny)
library(leaflet)
library(readr)
library(geosphere)

# library(bslib) must be installed using terraform

# COMMAND ----------

# set up the spark connection (using sparklyr pkg)
sc <- spark_connect(method = "databricks")

# load the vertices dataset (which is in delta format)
nodes_original <- spark_read_delta(sc,
                                   path = paste0("/mnt/public/clean/",
                                               "fincen-graph/vertices"))

# let´s take a look
head(nodes_original)


# COMMAND ----------

# load the original edges dataset (which is in delta format)
edges_original <- as_tibble(
  spark_read_delta(sc,
                   path = paste0("/mnt/public/", "clean/fincen-graph/edges")))

# let´s take a look
head(edges_original)


# COMMAND ----------

# read the spark data frame with iso codes and geo-locations of
# all the nations of the world

iso_codes <- spark_read_csv(sc,
                           path = "/mnt/public/tools/countries-codes.csv",
                           delimiter = ";",
                           header = T)

# select and rename only:

#"ISO3 CODE"--> the ISO code of each country. New name is "iso_code"
#"LABEL EN" --> the name of the countries in English. New name is "country"
#"geo_point_2d" --> the geo coordinates of the countries.
# New name is "location"

iso_data <-  iso_codes %>%

  select(3, 7, 11) %>%

  rename(iso_code = ISO3_CODE, country = LABEL_EN, location = geo_point_2d)

# some countries do not have the geo-location, we delete them

iso_data <- iso_data %>% filter(!is.na(location))


# COMMAND ----------

# the "location" variable holds both coordinates in string form in a single
# feature, split them and make 2 features

iso_data <- iso_data %>%

  mutate(loc_middle = split(location, ",")) %>%  #split the original location

  as_tibble() %>%   #make it a "tibble" (a dplyr format)

  mutate(lat = map_chr(loc_middle, 1))  %>% # create latitude

  mutate(long = purrr::map_chr(loc_middle, 2)) %>%  #create the longitude

  select(- loc_middle)  %>% #delete the matrix feature created during the split

  select(- location) # delete the original location feature


#lat and long feature are characterers, make them double

iso_data$lat <- as.numeric(iso_data$lat)

iso_data$long <- as.numeric(iso_data$long)

#let´s have a look
head(iso_data)

# COMMAND ----------

#load the clean fincen dataset and retrieve the iso code attached
#to each country

fincen <- spark_read_delta(sc, path = "/mnt/public/clean/fincen")

#combinie country and iso code fro sender and benificary banks and take
#the unique rows so to build a list of all observed countries

country_list <- bind_rows(
  as_tibble(

    fincen %>%

      distinct(sender_country,

               sender_iso) %>%

      rename(country = sender_country, iso_code = sender_iso)),

  as_tibble(fincen %>%
              distinct(beneficiary_country, beneficiary_iso) %>%

              rename(country = beneficiary_country,

                     iso_code = beneficiary_iso))) %>%

  distinct(country, iso_code)

#take a look
head(country_list)


# COMMAND ----------

# add to "node_original" the ISO_CODE related to each observed country

node_data <- nodes_original %>%

  left_join(country_list, copy = TRUE) %>%

  arrange(id)

# let´s take a look

node_data %>% print(n = 40)

# COMMAND ----------

# attach longitude and latitue to node_final using the iso_code dataset
node_data <- node_data %>%

  left_join(iso_data, by = "iso_code", copy = TRUE) %>%

  arrange(id) %>%

  select(- country_y) %>%

  rename(country = country_x)

# take a look
node_data

# COMMAND ----------

# it looks like that in the iso_data the coordinates for Curacao region
# is no present: add it manually

node_data <- node_data %>% na.replace(lat = 12.169570)

node_data <- node_data %>% na.replace(long = - 68.990021)


# COMMAND ----------

# check if each bank in each country is observed only once

# which bank in a country is observed more than once?
node_data %>% count(bank, country) %>% filter(n > 1) # no observations selected

# COMMAND ----------

# transform node_data in tibble

node_data <- node_data %>% as_tibble()

head(node_data)

# COMMAND ----------

# load "results_data": the results from the link prediction model

# columns:
# 1. src: node_id from where the edge starts (start at 0)
# 2. dst: node_id to where the edge ends (start at 0)
# 3. score: the estimated probability of connection btw src and dst
# 4. label: true connection status

results_data <- read_csv(

  "/dbfs/mnt/public/clean/fincen-graph/results_link_prediction.csv")

# take a look
results_data

# COMMAND ----------

# append to "results_data"  latitude and longitude for src and dst nodes

results_data <- results_data %>%

  add_column(

    src_lat = node_data$lat[results_data$src + 1],

    dst_lat = node_data$lat[results_data$dst + 1],

    src_long = node_data$long[results_data$src + 1],

    dst_long = node_data$long[results_data$dst + 1])

# the edges stored here refer to both positive and negative examples:
# for displaying we only keep the positive edges

results_data <- results_data %>%

  filter(label == 1) %>%

  select(- label) %>%

  as_tibble()

results_data

# COMMAND ----------

# the edges in "results_data" only concern a sub-set of nodes of the
# original graph - keep only the involved nodes

node_data <- node_data %>%

  filter(id %in% sort(unique(c(results_data$src, results_data$dst))))

# COMMAND ----------

# add name of the "src" and "dst" bank  for each observed edge

results_data <- results_data %>%

  left_join(select(node_data, id, bank), by = c("src" = "id")) %>%

  rename(src_bank = bank)

results_data <- results_data %>%

  left_join(select(node_data, id, bank), by = c("dst" = "id")) %>%

  rename(dst_bank = bank)

# COMMAND ----------

#select list of banks obsered as "src" of an edge

src_banks_id <- as.numeric(results_data$src)

# COMMAND ----------

#append two columns (amount, transactions) to results_data
results_data <- results_data %>% add_column(amount = 0, transactions = 0)

for (i in 1:seq_len(nrow(results_data))) {

  #selected ending points of the edge
  point_1 <- as.numeric(results_data[i, "src"])
  point_2 <- as.numeric(results_data[i, "dst"])

 #select the row of "edges_original" where the edge has been observed
 selected_row <- which((

   edges_original$src == point_1 & edges_original$dst == point_2) |

     (edges_original$src == point_2 & edges_original$dst == point_1))[1]

results_data[i, c(c("amount", "transactions"))] <- edges_original[

  selected_row, c("amount_transactions", "number_transactions")]

}

results_data

# COMMAND ----------

# add an html formatted column to results_data with info about transations to
# be shown as popups when crossing and edge with the mouse

results_data_popup <- results_data %>%

  add_column(popup = paste0("<b>amount: </b>",
                            paste(format(
                              round(
                                results_data$amount / 1e3, 1),
                              trim = TRUE), "K $"),
                            "<br>",
                            "<b>transactions: </b>",
                            results_data$transactions))

# COMMAND ----------

ui <- fluidPage(

  # theme = bslib::bs_theme(bootswatch = "darkly"),

  # header of the shiny app
  titlePanel("Link prediction with Shiny"),

  # position of the click buttons
  sidebarLayout(position = "left",

                sidebarPanel(

                  ## UI filters -----------------------------
                  h4("Choose Country and Bank"),

                  # box with choices to select from
                  selectInput("country_filter",
                                     label = "Country",
                                     multiple = TRUE,
                                     choices = unique(sort(node_data$country))),
                  br(),
                  checkboxGroupInput("bank_filter",
                                     label = "Bank"),

                  # button with focus point
                  br(),
                   h4("Focus points"),
                  actionButton("button1", "Laundry Hubs")

                ),

                mainPanel(

                   ## Interactive map
                  leafletOutput("map"),

                   tableOutput("data")
                  )
              )
        )

server <- shinyServer(function(input, output, session) {

 # create a reactive value "but1" such that when we click on the action
 # button it becomes TRUE and when we re-click it become FALSE

 but1 <- reactiveValues(but1 = FALSE)

 observeEvent(input$button1,

              isolate({
                but1$but1 <- !but1$but1
                }
                )
              )

  # This section updates "bank_filter" based on the chosen "country_filter"

  observe({

    #select rows of node data corresponding to the selected countries
    country_data <- node_data %>%

                      filter(country %in% input$country_filter) %>%

                      filter(id %in% src_banks_id)

    # select banks placed in the selected countries
    bank_data <- unique(sort(country_data$bank))

    # update te checkbox input
    updateCheckboxGroupInput(session,
                            "bank_filter",
                            label = "Bank",
                            choices = bank_data
                            )

  })

  # create a subset of  "node_data" based on the chosen country and bank filter

  filtered_data <- reactive({

    node_data %>%

      filter(country %in% input$country_filter) %>%
      filter(bank %in% input$bank_filter)
    })


   # select the edges having as "src node" one of the nodes selected above

     filtered_edges <-  reactive({

      results_data_popup %>% filter(src %in% filtered_data()$id)

     })


  ## CREATE LAUNDRY HUBS dataset

  # which "dst" nodes are observed in most edges?

  group_count <- results_data_popup %>% count(dst) # count per dst node

  group_amount <- results_data_popup %>%

    group_by(dst) %>%

    summarise(amount = sum(amount))  #sum of amount per dst node

  laundry_hub  <- arrange(

    group_count %>%

      left_join(group_amount, by = "dst"), desc(n)) %>%

    left_join(node_data, by = c("dst" = "id")) %>%

    relocate(bank) %>%

    rename(id = dst)


  # select the first 2 (if there are more than 2)

  if (nrow(laundry_hub) > 2) {

    laundry_hub <- laundry_hub[1:2, ]

  }

  ## selected edges ending in one of the laundry hubs

  # select all the edge having as dst nodes one the first 2 laundry hubs

  edges_hub <- results_data_popup %>% filter(dst %in% laundry_hub$id)

  # make curved lines for the hub edges

  coordinate_src_hub <- edges_hub %>% select(src_long, src_lat) %>% as.matrix

  coordinate_dst_hub <- edges_hub %>% select(dst_long, dst_lat) %>% as.matrix

    curved_lines_hub <- gcIntermediate(

       p1 = coordinate_src_hub,

       p2 = coordinate_dst_hub,

       breakAtDateLine = TRUE,

       addStartEnd = TRUE,

       sp = TRUE)


###### CURVE for vanlig MAP

  coordinate_src <- reactive({

    filtered_edges() %>% select(src_long, src_lat) %>% as.matrix

    })

  coordinate_dst <- reactive({

    filtered_edges() %>% select(dst_long, dst_lat) %>% as.matrix

    })

   curved_lines <- reactive({

     gcIntermediate(

       p1 = coordinate_src(),

       p2 = coordinate_dst(),

       breakAtDateLine = TRUE,

       addStartEnd = TRUE,

       sp = TRUE
  )

    })

  ## Render the table

  output$data <- renderTable({

    if (but1$but1) {

        laundry_hub

    }else{

      prova <- filtered_edges() %>%

        select(src_bank, dst_bank, amount, transactions, score)

      prova

    }
  })

# CREATE MAP

  output$map <- renderLeaflet({

   map <- leaflet(options = leafletOptions(minZoom = 0)) %>%

     setMaxBounds(lng1 = - 180,
                  lat1 = - 89.98155760646617,
                  lng2 = 180,
                  lat2 = 89.99346179538875) %>%

    addProviderTiles(providers$OpenStreetMap.France)

    ## add edges btw banks

    if (nrow(filtered_edges()) == 0) {

    }else{

        map <- addPolylines(map,
                         data = curved_lines(),
                         weight = 2,
                         label = lapply(filtered_edges()$popup,
                                        htmltools::HTML),
                         labelOptions = labelOptions(
                           noHide = ifelse(nrow(
                             filtered_edges()) > 0 & nrow(
                               filtered_edges()) < 3, TRUE, FALSE))
                         )

      map <- addCircleMarkers(map,
                            data = filtered_edges(),
                            lng = ~ dst_long,
                            lat = ~ dst_lat,
                            radius = 2,
                            color = "blue",
                            clusterOptions = markerClusterOptions(),
                            label = filtered_edges()$dst_bank,
                            labelOptions = labelOptions(
                              noHide = ifelse(nrow(
                                filtered_edges()) > 0 & nrow(
                                  filtered_edges()) < 3, TRUE, FALSE))
                            )

      map <- addCircleMarkers(map,
                             data = filtered_edges(),
                             lng = ~ src_long,
                             lat = ~ src_lat,
                             radius = 2,
                             color = "red",
                             clusterOptions = markerClusterOptions(),
                             label = filtered_edges()$src_bank,
                             labelOptions = labelOptions(
                               noHide = ifelse(nrow(
                                 filtered_edges()) > 0 & nrow(
                                   filtered_edges()) < 3, TRUE, FALSE))
                             )

    }

    ### MAP HUBS

    map_hubs <- leaflet(options = leafletOptions(minZoom = 0)) %>%

    setMaxBounds(lng1 = - 180,
                 lat1 = - 89.98155760646617,
                 lng2 = 180,
                 lat2 = 89.99346179538875) %>%

    addProviderTiles(providers$OpenStreetMap.France)

    map_hubs <- addPolylines(map_hubs,
                         data = curved_lines_hub,
                         weight = 2,
                         label = lapply(edges_hub$popup, htmltools::HTML))

    map_hubs <- addCircleMarkers(map_hubs,
                            data = edges_hub,
                            lng = ~ dst_long,
                            lat = ~ dst_lat,
                            radius = 2,
                            color = "blue",
                            clusterOptions = markerClusterOptions(),
                            label = edges_hub$dst_bank)

      map_hubs <- addCircleMarkers(map_hubs,
                             data = edges_hub,
                             lng = ~ src_long,
                             lat = ~ src_lat,
                             radius = 2,
                             color = "red",
                             clusterOptions = markerClusterOptions(),
                             label = edges_hub$src_bank)

 if (but1$but1) {

   map_hubs

}else{

      map

    }

  })

 }
)

shinyApp(ui, server)
