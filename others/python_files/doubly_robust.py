class data_preprocessing:
    def __init__(self, df, features=None):
        if features:
            self.df = df[features]
        else:
            self.df = df
        unfeatures_col = ['USER_ID', 'LABEL', 'RESPONSE', 'treatment']
        numeric_features = \
            [col for col in self.df.dtypes.index \
             if ('int' in str(self.df.dtypes[col])
                or 'float' in str(self.df.dtypes[col]))
             and col not in unfeatures_col]
        cat_features = \
            [col for col in self.df.dtypes.index
             if 'object' in str(self.df.dtypes[col])
             and col not in unfeatures_col]
        other_type_features = \
            [col for col in self.df.dtypes.index
             if col not in numeric_features + cat_features
                and col not in unfeatures_col]
        self.numeric_features = numeric_features
        self.cat_features = cat_features
        self.other_type_features =other_type_features

    def _pipeline(self):

        numeric_pipeline = Pipeline(
            steps=[
            ("numeric_feature_generator", SimpleImputer(strategy="mean")),
            ("numerica_standard", StandardScaler()),
            ])
        categorical_pipeline = Pipeline(

            steps=[
                (
                    "categorical_generator",
                    SimpleImputer(strategy="constant", fill_value="None"),
                ),
                ("categorical_encoder", OneHotEncoder(sparse_output=False, categories="auto")),
            ]
        )

        preprocessor = ColumnTransformer(
            transformers=[
                ("num_features", numeric_pipeline, self.numeric_features),
                ("cat_features", categorical_pipeline, self.cat_features),
            ]
        )
        return preprocessor

    def _get_processor(self):
        preprocessor = self._pipeline()
        df_handled = preprocessor.fit_transform(self.df)
        all_columns = self.numeric_features + list(preprocessor.named_transformers_['cat_features'].get_feature_names_out(self.cat_features))
        dfupdate = pd.DataFrame(df_handled, columns=all_columns)
        return dfupdate


    def _sanity_check(self):
        # create a treatment column
        self.df = self.df.assign(treatment=np.where(self.df['LABEL']=='control', 0, 1))
        # check control and test data volume distrubtion
        total_volume = self.df.shape[0]
        total_features = self.df.shape[1] - 4
        if total_features <= 0:
            print("You don't have features, so no need to use causal inference")
            return False
        print("There are total {0} records, and {1} features".format(total_volume, total_features))
        # check whether control and treatment exist
        treatments_num = self.df['treatment'].nunique()
        print("There are total {0} different treatments".format(treatments_num - 1))
        # check different treatment volume
        treatment_group = self.df.groupby(['treatment']).agg({'USER_ID':'count'}).reset_index()
        print("The treatment(s) / control data volume distribution is as below")
        print(treatment_group)

        # check features
        numeric_feature_count = len(self.numeric_features)
        cat_feature_count = len(self.cat_features)
        print("There are total {0} numeric features".format(numeric_feature_count))
        print("There are total {0} categorical features".format(cat_feature_count))
        print("The numeric features are ")
        print(",".join(self.numeric_features))
        print("The cat features are ")
        print(",".join(self.cat_features))

        if len(self.other_type_features) > 0:
            print("Please check these features which are not numeric or categorical " + ",".join(self.other_type_features))
            return False
        return True


    def preprocessing(self):
        if self._sanity_check():
            return self._get_processor(), self.df['treatment'], self.df['RESPONSE']
        else:
            return None, None, None


class doubly_robust:
    def __init__(self):
        self.dr = None

    def _lgbmodel(self, binary_response=True):
        lgb_T_XZ_params = {
            'objective' : 'binary',
            'metric' : 'auc',
            'learning_rate': 0.1,
            'num_leaves' : 30,
            'max_depth' : 5
        }
        model_T_XZ = lgb.LGBMClassifier(**lgb_T_XZ_params)
        if binary_response:
            lgb_Y_X_params = {
                'objective' : 'binary',
                'metric' : 'auc',
                'learning_rate': 0.1,
                'num_leaves' : 30,
                'max_depth' : 5
            }
            model_Y_X = lgb.LGBMClassifier(**lgb_Y_X_params)
        else:
            lgb_Y_X_params = {
                'metric' : 'rmse',
                'learning_rate': 0.1,
                'num_leaves' : 30,
                'max_depth' : 5
            }
            model_Y_X = lgb.LGBMRegressor(**lgb_Y_X_params)
        return model_Y_X, model_T_XZ
    def check_response_type(self, Y):
        y_nunique = np.sort(Y.unique())
        if len(y_nunique) == 2 and (y_nunique[0] == 0 and y_nunique[1] == 1):
            binary_response = True
        else:
            binary_response = False
        return binary_response

    def doubly_robust_model(self, df_cofounder, T_treatment, Y_response):
        binary_response = self.check_response_type(Y_response)
        outcome, propensity = self._lgbmodel(binary_response)
        ipw = IPW(propensity, clip_min=0.05, clip_max=0.95)
        std = StratifiedStandardization(outcome)
        dr = AIPW(std, ipw)
        dr.fit(df_cofounder, T_treatment, Y_response)
        return dr

    def train_model(self, Y_response, T_treatment, df_cofounder):
        dr = self.doubly_robust_model(df_cofounder, T_treatment, Y_response)
        pop_outcome = dr.estimate_population_outcome(df_cofounder, T_treatment, Y_response)
        return pop_outcome



boostrap_results = evaluation.evaluate_bootstrap(
    dr,
    X_cofounder,
    treatment,
    response,
    n_bootstrap=1000,
    n_samples=None,
    replace=True,
    refit=False,
    metrics_to_evaluate=None
)

class doubly_robust:
    def __init__(self):
        self.ipw = None
        self.outcome = None
        self.dr = None

    def _lgbmodel(self, binary_response=True):
        lgb_T_XZ_params = {
            'objective' : 'binary',
            'metric' : 'auc',
            'learning_rate': 0.1,
            'num_leaves' : 30,
            'max_depth' : 5
        }
        model_T_XZ = lgb.LGBMClassifier(**lgb_T_XZ_params)
        if binary_response:
            lgb_Y_X_params = {
                'objective' : 'binary',
                'metric' : 'auc',
                'learning_rate': 0.1,
                'num_leaves' : 30,
                'max_depth' : 5 
            }
            model_Y_X = lgb.LGBMClassifier(**lgb_Y_X_params)
        else:
            lgb_Y_X_params = {
                'metric' : 'rmse',
                'learning_rate': 0.1,
                'num_leaves' : 30,
                'max_depth' : 5
            }
            model_Y_X = lgb.LGBMRegressor(**lgb_Y_X_params)
        return model_Y_X, model_T_XZ 
        
    def check_response_type(self, Y):
        y_nunique = np.sort(Y.unique())
        if len(y_nunique) == 2 and (y_nunique[0] == 0 and y_nunique[1] == 1):
            binary_response = True 
        else:
            binary_response = False
        return binary_response
        
    def doubly_robust_model(self, df_cofounder, T_treatment, Y_response):
        binary_response = self.check_response_type(Y_response)
        outcome, propensity = self._lgbmodel(binary_response)
        ipw = IPW(propensity, clip_min=0.05, clip_max=0.95)
        std = StratifiedStandardization(outcome)
        dr = AIPW(std, ipw)
        self.ipw = ipw 
        self.outcome = std 
        dr.fit(df_cofounder, T_treatment, Y_response)
        self.dr = dr
        return self

    def get_samples(self, df_cofounder, T_treatment, Y_response, seed):
        indices = resample(df_cofounder.index, 
                           replace=True, n_samples=int(df_cofounder.shape[0] * 0.9),
                           random_state=seed)
        x_sample = df_cofounder.iloc[indices].reset_index(drop=True)
        t_sample = T_treatment.iloc[indices].reset_index(drop=True)
        y_sample = Y_response.iloc[indices].reset_index(drop=True)
        return x_sample, t_sample, y_sample

    def bootstrap_model(self, dr, df_cofounder, T_treatment, Y_response, n_bootstrap=1000):
        estimates = []
        for seed in range(n_bootstrap):
            x_sample, t_sample, y_sample = self.get_samples(df_cofounder, T_treatment, Y_response, seed)
            dr.fit(x_sample, t_sample, y_sample)
            effect = dr.estimate_population_outcome(x_sample, t_sample, y_sample)
            ate = effect[1] - effect[0]
            estimates.append(ate)
        return np.array(estimates)
        
    def ate_estimate(self, df_cofounder, T_treatment, Y_response):
        if self.dr is None:
            self.doubly_robust_model(df_cofounder, T_treatment, Y_response)
        dr = self.dr
        estimates = self.bootstrap_model(dr, df_cofounder, T_treatment, Y_response)
        mean_ate = np.mean(estimates)
        ci_lower = np.percentile(estimates, 2.5)
        ci_upper = np.percentile(estimates, 97.5)
        return mean_ate, ci_lower, ci_upper

    def model_evaluation(self, df_cofounder, T_treatment, Y_response):
        mean_ate, ci_lower, ci_upper = self.ate_estimate(df_cofounder, T_treatment, Y_response)
        print("The average treatment effect is {0},\n2.5% lower bound {1},\n97.5% upper \
        bound is {2}".format(mean_ate, ci_lower, ci_upper))
        os.environ['LOKY_MAX_CPU_COUNT'] = '8'
        if self.dr is None:
            self.doubly_robust_model(df_cofounder, T_treatment, Y_response)
        dr = self.dr
        res = evaluate(dr, df_cofounder, T_treatment, Y_response, cv='auto')
        if 'ContinuousOutcomeEvaluationResults' in str(type(res)):
            # continuous accuracy
            plt.figure()
            res.plot_continuous_accuracy()
            plt.show()
            # common support
            plt.figure()
            res.plot_common_support()
            plt.show()
            #print("residue graph is ")
            res.plot_residuals()
            plt.show()
        else:
            #print("Roc of model is ")
            plt.figure()
            res.plot_roc_curve()
            plt.show()
            #print("Calibration graph is ")
            plt.figure()
            res.plot_calibration_curve()
            plt.show()
            plt.figure()
            res.plot_pr_curve()
            plt.show()
            
        return res

    def ipw_evaluation(self, X_cofounder, treatment, response):
        if self.ipw is not None:
            evaluate_ipw = evaluate(self.ipw, X_cofounder, treatment, response)
            #print(type(evaluate_ipw))
            #print("The love plot is ")
            plt.figure()
            evaluate_ipw.plot_covariate_balance(kind="love")
            plt.show()
            #print("The weight distribution graph is ")
            plt.figure()
            evaluate_ipw.plot_weight_distribution()
            plt.show()
            #print("ipw roc curve")
            plt.figure()
            evaluate_ipw.plot_roc_curve()
            plt.show()
        else:
            print("ipw doesn't run until now, please run model_evaluation firstly")