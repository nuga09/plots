
from singleScenarioPlot import PlottingScenario
from singleScenarioData import DataExtractionScenario
from scenarioCompPlot import PlotScenarioComparison
from scenarioCompData import DataExtractionScenarioComparison
from glob import glob
import random
import os, shutil
from datetime import datetime

if __name__ == "__main__":
    # file_path = r"C:\Users\ra.maier\sciebo\Exchange\exchange_Nuga\result_data_TagDerNeugier\Evaluation_Results_S2_Expansive_IA800_OA600.xlsx"
   
    
    global_file_path = '/storage/internal/home/d-nuga/nestor/nestor/data/Results/2022-09-*/Evaluation_Results_new_scenario.xlsx' 
    
    files = glob(global_file_path)
    
    datetime_str = '22-09-01'
    datetime_object = datetime.strptime(datetime_str, '%y-%m-%d').date()
    files_for_plots = []
    # print(files)
    for file in files:
        fs = os.path.dirname(file)
        # print(fs)
        a = os.path.basename(fs) 
        file_date = datetime.strptime(str(a[0:10]), '%Y-%m-%d').date()
        if datetime.strptime(str(a[0:10]), '%Y-%m-%d').date() >= datetime_object:
            files_for_plots.append(file)
        
    file_path = random.choice(files_for_plots)
    # print(file_path)
    # print(files_for_plots)
    emp=[]
    # files_for_plots = random.sample(files_for_plots, k=3)
    for  count,x in enumerate(range(0, len(files_for_plots), 3)):
        emp.append({count:files_for_plots[x:x+3]})

    files_for_plots = emp[0][0]
    files_for_plots_1 = emp[1][1]
    files_for_plots_2 =  emp[2][2]
    files_for_plots_3 =  emp[3][3]
    files_for_plots_4 =  emp[4][4]

    print(len(emp),'\n', files_for_plots,'\n', files_for_plots_1,'\n',files_for_plots_2,'\n',files_for_plots_3, '\n',files_for_plots_4 )
    

        
        

    
    # A) JUST GET DATA
    data_sc = DataExtractionScenario(file_path)
    data_sc.data_primary_energy()
    data_sc.data_import_quota()
    
    # B) DO SOME PLOTTING
    plt_sc = PlottingScenario(file_path, fig_size=(14,8))
    plt_sc.fig_import()
    plt_sc.fig_installed_cap()
    plt_sc.fig_electricity_generation()
    plt_sc.fig_storage()

    plt_sc.fig_cost()
    plt_sc.fig_CO2()
    # ... more available
    # ... more to be implemented


    data_compsc = DataExtractionScenarioComparison(files_for_plots_1)
    data_compsc.data_primary_energy()
    data_compsc.data_import_quota()

    plt_coompsc = PlotScenarioComparison(files_for_plots_1)
    plt_coompsc.fig_installed_cap()
    plt_coompsc.fig_cost()
    plt_coompsc.fig_CO2()
    plt_coompsc.fig_storage()
    plt_coompsc.fig_imports()
    plt_coompsc.fig_electricity_generation()

    # plt_compsc = PlotScenarioComparison(comparison_list)
