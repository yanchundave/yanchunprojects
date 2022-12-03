use_condaenv("r-reticulate")
library(Robyn)
Sys.setenv("R_FUTURE_FORK_ENABLE"="true")
options(future.fork.enable = TRUE)
setwd("/Users/yanchunyang/Documents/datafiles/Rfile/")
#user
dt_2 = read.csv("/Users/yanchunyang/Documents/datafiles/pystan/user/mmm_r_raw_one.csv")

tail(dt_2)
data("dt_prophet_holidays")
##robyn_object <- "/Users/yanchunyang/Documents/datafiles/Rfile/Robyn_update.RDS"
robyn_object_advance <- "/Users/yanchunyang/Documents/datafiles/Rfile/advance/Robyn_update.RDS"
## pay attention to apple search ads column
InputCollect <- robyn_inputs(
  dt_input = dt_2
  ,dt_holidays = dt_prophet_holidays
  ,date_var="date" # date format must be "2020-01-01"
  ,dep_var="PV" # there should be only one dependent variable
  #user
  #,dep_var_type = "conversion" # "revenue" or "conversion"
  #revenue
  ,dep_var_type = "revenue" # "revenue" or "conversion"
  ,prophet_vars = c("trend", "season", "weekday") # "trend","season", "weekday" & "holiday"
  ,prophet_signs = c("default","default", "default")
  ,prophet_country = "US"# input one country. dt_prophet_holidays includes 59 countries by default
  ,context_vars = c('competition', 'inflation')# e.g. competitors, discount, unemployment etc
  ,context_signs = c('default', 'default')
  ,paid_media_spends = c('Snapchat_Android', 'bytedanceglobal_int_Android',
                         'bytedanceglobal_int_iOS', 'Snapchat_iOS', 'Adwords_iOS',
                         'Apple_Search_Ads_iOS', 'Tatari_TV', 'Facebook_Android', 'Facebook_iOS',
                         'Adwords_Android',  'minor_channels', 'new_channels') # mandatory input
  ,paid_media_vars = c('Snapchat_Android', 'bytedanceglobal_int_Android',
                       'bytedanceglobal_int_iOS', 'Snapchat_iOS', 'Adwords_iOS',
                       'Apple_Search_Ads_iOS', 'Tatari_TV', 'Facebook_Android', 'Facebook_iOS',
                       'Adwords_Android',  'minor_channels', 'new_channels') # mandatory.
  ,paid_media_signs = c('positive','positive','positive','positive','positive','positive','positive',
                        'positive','positive','positive','positive', 'positive')
  # paid_media_vars must have same order as paid_media_spends. Use media exposure metrics like
  # impressions, GRP etc. If not applicable, use spend instead.
  ,organic_vars = NULL# marketing activity without media spend
  ,factor_vars = NULL # specify which variables in context_vars or organic_vars are factorial
  ,window_start = "2022-01-01"
  ,window_end = "2022-10-22"
  ,adstock = "weibull_cdf" # geometric, weibull_cdf or weibull_pdf.
  
)
print(InputCollect)
hyper_names(adstock = InputCollect$adstock, all_media = InputCollect$all_media)
plot_adstock(plot = FALSE)
plot_saturation(plot = FALSE)
hyperparameters <- list(
  Snapchat_Android_alphas=c(0.5, 4),
  Snapchat_Android_gammas=c(0.3,1),
  Snapchat_Android_shapes=c(0.0001,2),
  Snapchat_Android_scales=c(0, 0.1),
  
  bytedanceglobal_int_Android_alphas=c(0.5, 4),
  bytedanceglobal_int_Android_gammas=c(0.3,1),
  bytedanceglobal_int_Android_shapes=c(0.0001,2),
  bytedanceglobal_int_Android_scales=c(0, 0.1),
  
  bytedanceglobal_int_iOS_alphas=c(0.5, 4),
  bytedanceglobal_int_iOS_gammas=c(0.3,1),
  bytedanceglobal_int_iOS_shapes=c(0.0001,2),
  bytedanceglobal_int_iOS_scales=c(0, 0.1),
  
  Snapchat_iOS_alphas=c(0.5, 4),
  Snapchat_iOS_gammas=c(0.3,1),
  Snapchat_iOS_shapes=c(0.0001,2),
  Snapchat_iOS_scales=c(0, 0.1),
  
  Adwords_iOS_alphas=c(0.5, 4),
  Adwords_iOS_gammas=c(0.3,1),
  Adwords_iOS_shapes=c(0.0001,2),
  Adwords_iOS_scales=c(0, 0.1),
  
  Apple_Search_Ads_iOS_alphas=c(0.5, 4),
  Apple_Search_Ads_iOS_gammas=c(0.3,1),
  Apple_Search_Ads_iOS_shapes=c(0.0001,2),
  Apple_Search_Ads_iOS_scales=c(0, 0.1),
  
  Tatari_TV_alphas=c(0.5, 4),
  Tatari_TV_gammas=c(0.3,1),
  Tatari_TV_shapes=c(0.0001,2),
  Tatari_TV_scales=c(0, 0.1),
  
  Facebook_Android_alphas=c(0.5, 4),
  Facebook_Android_gammas=c(0.3,1),
  Facebook_Android_shapes=c(0.0001,2),
  Facebook_Android_scales=c(0, 0.1),
  
  Facebook_iOS_alphas=c(0.5, 4),
  Facebook_iOS_gammas=c(0.3,1),
  Facebook_iOS_shapes=c(0.0001,2),
  Facebook_iOS_scales=c(0, 0.1),
  
  Adwords_Android_alphas=c(0.5, 4),
  Adwords_Android_gammas=c(0.3,1),
  Adwords_Android_shapes=c(0.0001,2),
  Adwords_Android_scales=c(0, 0.1),
  
  minor_channels_alphas=c(0.5, 4),
  minor_channels_gammas=c(0.3,1),
  minor_channels_shapes=c(0.0001,2),
  minor_channels_scales=c(0, 0.1),
  
  new_channels_alphas=c(0.5, 4),
  new_channels_gammas=c(0.3,1),
  new_channels_shapes=c(0.0001,2),
  new_channels_scales=c(0, 0.1),
)
InputCollect <- robyn_inputs(InputCollect = InputCollect, hyperparameters = hyperparameters)
print(InputCollect)
OutputModels <- robyn_run(
  InputCollect = InputCollect # feed in all model specification
  #, cores = NULL # default
  #, add_penalty_factor = FALSE # Untested feature. Use with caution.
  , iterations = 2000 # recommended for the dummy dataset
  , trials = 5 # recommended for the dummy dataset
  , outputs = FALSE # outputs = FALSE disables direct model output
)
print(OutputModels)

## Check MOO (multi-objective optimization) convergence plots
OutputModels$convergence$moo_distrb_plot
OutputModels$convergence$moo_cloud_plot
# check convergence rules ?robyn_converge

## Calculate Pareto optimality, cluster and export results and plots. See ?robyn_outputs
OutputCollect <- robyn_outputs(
  InputCollect, OutputModels
  , pareto_fronts = 1
  # , calibration_constraint = 0.1 # range c(0.01, 0.1) & default at 0.1
  , csv_out = "pareto" # "pareto" or "all"
  , clusters = TRUE # Set to TRUE to cluster similar models by ROAS. See ?robyn_clusters
  , plot_pareto = TRUE # Set to FALSE to deactivate plotting and saving model one-pagers
  , plot_folder = robyn_object_advance # path for plots export
)
print(OutputCollect)


select_model <- "5_200_2" # select one from above
ExportedModel <- robyn_save(
  robyn_object = robyn_object_advance # model object location and name
  , select_model = select_model # selected model ID
  , InputCollect = InputCollect
  , OutputCollect = OutputCollect
)
print(ExportedModel)
write.csv(ExportedModel$summary, "/Users/yanchunyang/Documents/datafiles/Rfile/advance/R_metrics_advance_1.csv")

for (i in 1:length(ExportedModel$summary$rn)){
  print(i)
  print(paste("/Users/yanchunyang/Documents/datafiles/Rfile/advance/",gsub(" ", "",ExportedModel$summary$rn[i]),".png",sep=""))
  print(ExportedModel$summary$rn[i])
  print(ExportedModel$summary$mean_spend[i])
  print(dev.cur())
  t <- robyn_response(
    robyn_object = "/Users/yanchunyang/Documents/datafiles/Rfile/advance/Robyn_update.RDS",
    media_metric = ExportedModel$summary$rn[i],
    metric_value = ExportedModel$summary$mean_spend[i]
  )
  
  png(file<-paste("/Users/yanchunyang/Documents/datafiles/Rfile/advance/",gsub(" ", "",ExportedModel$summary$rn[i]),".png",sep=""))
  plot(t$plot)
  dev.off()
}

#### Marginal Response of different channels
margin_total <- c()
for(k in 1:10){
  marginal <- c()
  for (i in 1:length(ExportedModel$summary$rn)){
    
    t1 <- robyn_response(
      robyn_object = "/Users/yanchunyang/Documents/datafiles/Rfile/advance/Robyn_update.RDS",
      media_metric = ExportedModel$summary$rn[i],
      metric_value = ExportedModel$summary$mean_spend[i]
    )
    
    t2 <- robyn_response(
      robyn_object = "/Users/yanchunyang/Documents/datafiles/Rfile/advance/Robyn_update.RDS",
      media_metric = ExportedModel$summary$rn[i] ,
      metric_value = ExportedModel$summary$mean_spend[i] * (1 + 0.05 * k )
    )
    
    margin<- (t2$response - t1$response) /(ExportedModel$summary$mean_spend[i] * 0.05 * k)
    marginal[i] <- margin
  }
  if(k == 1) {
    margin_total <- marginal
  }
  else{
    margin_total <-rbind(margin_total, marginal)
  }
}
margin_df <-data.frame(margin_total)
write.csv(margin_df, file="/Users/yanchunyang/Documents/datafiles/Rfile/advance/margin_total.csv")
options(scipen = 100)
print(ExportedModel$summary$rn)
print(marginal)



###########select best model to output
#select_model <- "1_125_6" # select one from above
#ExportedModel <- robyn_save(
#  robyn_object = robyn_object # model object location and name
#  , select_model = select_model # selected model ID
#  , InputCollect = InputCollect
#  , OutputCollect = OutputCollect
#)
#print(ExportedModel)
#write.csv(ExportedModel$summary, "/Users/yanchunyang/Documents/datafiles/Rfile/R_metrics_advance.csv")


calibration_input <- data.frame(
  channel = c("Facebook"),
  liftStartDate=as.Date("2022-05-01"),
  liftEndDate = as.Date("2022-06-01"),
  liftAbs = c(1000),
  spend=(60000),
  confidence = c(0.85),
  metric = c("conversion")
)
