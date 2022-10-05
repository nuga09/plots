"""Module to import data of a comparison of nestor scenarios"""

from asyncio import protocols
from turtle import color
import matplotlib.cm as cm
from glob import glob
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
from scenarioCompData import DataExtractionScenarioComparison
import matplotlib.pyplot as plt



class PlotScenarioComparison():
    def __init__(self, path_list, fig_save_path=None,name=None):
        self.path_list = path_list

        # checks
        if not isinstance(path_list, list):
            raise ValueError("path_list needs to be a list of paths")
        for path in path_list:
            if not os.path.isfile(path):
                raise ValueError(f"Path '{path}' is not leading to a file")

        # raise NotImplementedError(
        #     "A scenario comparison is not yet implemented")
        if name is None:
            self.name = os.path.basename(path)
        else:
            self.name = name
        if fig_save_path == None:
            path = path = os.path.dirname(os.path.abspath(__file__))
            self.fig_save_path = os.path.join(
                path, "plots", self.name.replace(".xlsx", ""))
            if not os.path.isdir(self.fig_save_path):
                os.mkdir(self.fig_save_path)
        else:
            raise NotImplementedError()
    
    def fig_installed_cap(self):
        data = DataExtractionScenarioComparison(self.path_list).data_installed_cap()
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=0.3, 
                        ax=ax, position=count) 
        ax.set_ylabel('Solar & Wind Energy Installed capacities [GW]')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns, loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_Solar_wind installed_cap.png')
        plt.savefig(save_path)

    def fig_cost(self):
        data = DataExtractionScenarioComparison(self.path_list).data_cost()
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=0.3, 
                        ax=ax, position=count) 
        ax.set_ylabel('Cost bn €/a')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns,loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_cost.png')
        plt.savefig(save_path)

    def fig_CO2(self):
        data = DataExtractionScenarioComparison(self.path_list).data_CO2()
        colors=['aqua', 'blue', 'fuchsia', 'gray', 'green', 'lime', 'maroon', 'navy', 'olive', 'purple', 'red', 'silver', 'teal', 'white',  'yellow']
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=0.3, 
                        ax=ax,color=colors, position=count) 

        ax.set_ylabel('Emissions GHG-Emissions [Kt CO2eq]')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns , loc='center left', bbox_to_anchor=(1, 0.5), ncol=2)
       
        
       
        # fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_CO2.png')
        plt.savefig(save_path)

    def fig_storage(self):
        data = DataExtractionScenarioComparison(self.path_list).data_storage()
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=0.3, 
                        ax=ax, position=count) 
        ax.set_ylabel(' Energy Storage [GW]')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns, loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_storage.png')
        plt.savefig(save_path)

    def fig_imports(self):
        data = DataExtractionScenarioComparison(self.path_list).data_import_quota()
        print(data)
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
       
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=0.3, 
                        ax=ax, position=count) 
        ax.set_ylabel(' Energy imports [TWH]')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns,loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_import.png')
        plt.savefig(save_path)

    def fig_electricity_generation(self):
        data = DataExtractionScenarioComparison(self.path_list).data_electricity_generation()
        print(data,'\n')
        for t in data:
            print(t.sum(axis=1).pct_change())
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=0.3, 
                        ax=ax, position=count) 
        ax.set_ylabel(' Primary energy use in GWH')
        ax.set_title(f'scenario comparison: \n{cases[0]} \n {cases[1]}\n {cases[2]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns, loc='center left', bbox_to_anchor=(1, 0.5))
        # ax.legend(dat.columns, loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_Electricity.png')
        plt.savefig(save_path)

         

        


    
   


   