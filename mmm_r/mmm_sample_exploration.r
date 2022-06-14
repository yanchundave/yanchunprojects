library("Robyn")
library("reticulate")
#use_python("~/miniforge3/envs/r-reticulate/bin/python3")
use_condaenv("r-reticulate")
Sys.setenv("R_FUTURE_FORK_ENABLE"="true")
options(future.fork.enable = TRUE)
setwd("/Users/yanchunyang/Documents/datafiles/Rfile/")
dt_2 = read.csv("/Users/yanchunyang/Documents/datafiles/mmm_r_advance.csv")
head(dt_2)
data("dt_prophet_holidays")
robyn_object <- "/Users/yanchunyang/Documents/datafiles/Rfile/Robyn_exploration.RDS"
InputCollect <- robyn_inputs(
  dt_input = dt_2
  ,dt_holidays = dt_prophet_holidays
  ,date_var="date" # date format must be "2020-01-01"
  ,dep_var="PV" # there should be only one dependent variable
  ,dep_var_type = "revenue" # "revenue" or "conversion"
  ,prophet_vars = c("trend", "season", "weekday", "holiday") # "trend","season", "weekday" & "holiday"
  ,prophet_country = "US"# input one country. dt_prophet_holidays includes 59 countries by default
  ,context_vars = NULL# e.g. competitors, discount, unemployment etc
  ,paid_media_spends = c('AA', 'AI', 'AS', 'LA', 'LI', 'RA', 'RI', 'SA', 'SI', 'TT', 'BA', 'BI', 'UN') # mandatory input
  ,paid_media_vars = c('AA', 'AI', 'AS', 'LA', 'LI', 'RA', 'RI', 'SA', 'SI', 'TT', 'BA', 'BI', 'UN') # mandatory.
  # paid_media_vars must have same order as paid_media_spends. Use media exposure metrics like
  # impressions, GRP etc. If not applicable, use spend instead.
  ,organic_vars = NULL# marketing activity without media spend
  ,factor_vars = NULL # specify which variables in context_vars or organic_vars are factorial
  ,window_start = "2021-03-01"
  ,window_end = "2022-04-08"
  ,adstock = "geometric" # geometric, weibull_cdf or weibull_pdf.
)
print(InputCollect)
hyper_names(adstock = InputCollect$adstock, all_media = InputCollect$all_media)
plot_adstock(plot = FALSE)
plot_saturation(plot = FALSE)
hyperparameters <- list(
  AA_alphas=c(0.5, 3)
  ,AA_gammas=c(0.3, 1)
  ,AA_thetas=c(0.1, 0.8)
  ,
  AI_alphas=c(0.5, 3)
  ,AI_gammas=c(0.3, 1)
  ,AI_thetas=c(0.1, 0.8)
  ,
  AS_alphas=c(0.5, 3)
  ,AS_gammas=c(0.3, 1)
  ,AS_thetas=c(0.1, 0.8)
  ,
  LA_alphas=c(0.5, 3)
  ,LA_gammas=c(0.3, 1)
  ,LA_thetas=c(0.1, 0.8)
  ,
  LI_alphas=c(0.5, 3)
  ,LI_gammas=c(0.3, 1)
  ,LI_thetas=c(0.1, 0.8)
  ,
  RA_alphas=c(0.5, 3)
  ,RA_gammas=c(0.3, 1)
  ,RA_thetas=c(0.1, 0.8)
  ,
  RI_alphas=c(0.5, 3)
  ,RI_gammas=c(0.3, 1)
  ,RI_thetas=c(0.1, 0.8)
  ,
  SA_alphas=c(0.5, 3)
  ,SA_gammas=c(0.3, 1)
  ,SA_thetas=c(0.1, 0.8)
  ,
  SI_alphas=c(0.5, 3)
  ,SI_gammas=c(0.3, 1)
  ,SI_thetas=c(0.1, 0.8)
  ,
  TT_alphas=c(0.5, 3)
  ,TT_gammas=c(0.3, 1)
  ,TT_thetas=c(0.1, 0.8)
  ,
  BA_alphas=c(0.5, 3)
  ,BA_gammas=c(0.3, 1)
  ,BA_thetas=c(0.1, 0.8)
  ,
  BI_alphas=c(0.5, 3)
  ,BI_gammas=c(0.3, 1)
  ,BI_thetas=c(0.1, 0.8)
  ,
  UN_alphas=c(0.5, 3)
  ,UN_gammas=c(0.3, 1)
  ,UN_thetas=c(0.1, 0.8)
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
  , plot_folder = robyn_object # path for plots export
)
print(OutputCollect)

###########select best model to output
select_model <- "1_139_10" # select one from above
ExportedModel <- robyn_save(
  robyn_object = robyn_object # model object location and name
  , select_model = select_model # selected model ID
  , InputCollect = InputCollect
  , OutputCollect = OutputCollect
)
print(ExportedModel)


print(ExportedModel)
write.csv(ExportedModel$summary, "/Users/yanchunyang/Documents/datafiles/Rfile/R_metrics_exploration.csv")
