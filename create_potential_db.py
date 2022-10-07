
import os, glob
import time
import pandas as pd
import numpy as np
import json
from multiprocessing.pool import Pool
from functools import reduce



from sqlalchemy import create_engine
from datetime import datetime
import lxml

FEDERAL_STATE_NAMING = \
    {
        "01": "Schleswig-Holstein",
        "02": "Hamburg",
        "03": "Niedersachsen",
        "04": "Bremen",
        "05": "Nordrhein-Westfalen",
        "06": "Hessen",
        "07": "Rheinland-Pfalz",
        "08": "Baden-Württemberg",
        "09": "Bayern",
        "10": "Saarland",
        "11": "Berlin",
        "12": "Brandenburg",
        "13": "Mecklenburg-Vorpommern",
        "14": "Sachsen",
        "15": "Sachsen-Anhalt",
        "16": "Thüringen"
    }

FEDERAL_STATES = ["01", "02", "03", "04", "05", "06", "07",
                  "08", "09", "10", "11", "12", "13", "14", "15", "16"]

exclusive_economic_zones = ["BalticSea_EEZ", "NorthSea_EEZ", "01", "02", "03", "13"]


naming_conversion_mapping={
        "onshore":"Wind",
        "offshore":"WindOffshore", # TODO check
        "openfield_pv":"OpenfieldPV",
        "rooftop_pv":"RooftopPV"# TODO
    }

    

class PotentialsToNestorDB():
    def __init__(self, trep_case_name, trep_db_path, nestor_ee_db_path, technology): # TODO also pass technology
        # check input data
        if technology not in ["onshore","offshore","openfield_pv","rooftop_pv"]:
            raise ValueError(
                f"Passed technology ({technology}) to 'PotentialsToNestorDB' is wrong. "+
            "Please either use ['onshore','offshore','openfield_pv','rooftop_pv']")

        # define variables
        self.case_name = trep_case_name
        self.trep_db_path = trep_db_path
        self.nestor_ee_db_path = os.path.join(nestor_ee_db_path,technology)
        self.technology = technology 
        self.nestor_ee_scenario_path = os.path.join(
            self.nestor_ee_db_path, trep_case_name)

        if not self.technology is 'rooftop_pv':
            # self.exclude_existing()
            self.prepare_nestor_data()
        else:
            # self.rooftop_potentials()
            self.prepare_nestor_data()

        

    def prepare_nestor_data(self, lifetime=20, weather_year=2014):
        # check if case exists
        self.historical_data_regions = pd.DataFrame()
        self.time_series = pd.DataFrame()
        self.potentials = pd.DataFrame(columns=["name", "profile", "lb", "ub"])
        self.fullloadhours = {}
        self.fullloadhours_stan = pd.Series()


        if self.technology=="offshore":
            regions=exclusive_economic_zones # TODO exclusive economic zones
       
        else:
            regions=FEDERAL_STATES # TODO federal states
        
        
        for region in regions:
            folder_path=os.path.join(
                self.nestor_ee_scenario_path,
                f"{naming_conversion_mapping[self.technology]}_{region}")
            report_path=os.path.join(
                self.nestor_ee_scenario_path,
                f"{naming_conversion_mapping[self.technology]}_{region}", 
                "report.json")
            if not os.path.isdir(folder_path) and not os.path.isfile(report_path):
                print(f"WARNING: region '{region}' missing!")
            else:
                print(f"region '{region}' exists!")
                
        self.region_availability = {}
        if self.technology=="openfield_pv":
            for region in regions:
                _region_path = os.path.join(
                    self.nestor_ee_scenario_path, f"{naming_conversion_mapping[self.technology]}_{region}")
                group_name = f"{naming_conversion_mapping[self.technology]}_{region}"
                # check regional availability
                self.region_availability[region] = {}
                # availability for potential
                self.region_availability[region]["potential"]= True
                # are existings there?
                _path_to_existing = os.path.join(
                    _region_path, f"existing_{naming_conversion_mapping[self.technology]}_{region}.csv")
                existing = pd.read_csv(_path_to_existing)
               
                
                
                self.region_availability[region]["existing"] =  len(existing) is not 0 #!               
            
                if (self.region_availability[region]["potential"] is False) and (self.region_availability[region]["existing"] is False):
                    print(f"No data for region {region}")
                    continue
            
                group_name = f"{naming_conversion_mapping[self.technology]}_{region}"

                if self.region_availability[region]["potential"]:
                    self.spatial_Tech_aggregation( region, _region_path)
                    self.prepare_historical_data(
                        _region_path, region, group_name, lifetime)
                    self.prepare_time_series(
                        _region_path, region, group_name, weather_year)
                    self.prepare_potentials(_region_path, group_name, region)
                    self.prepare_FLH(_region_path, region, group_name)
            # check and later TODO
            if self.time_series.min().min() < 0:
                print(
                    f"WARNING: Negative values for time-series. Min value {self.time_series.min().min()} Will be corrected to > 0.")
                self.time_series = self.time_series.clip(lower=0)
            if self.time_series.max().max() > 0:
                print(
                    f"WARNING: Values higher 1 for time-series. Max value {self.time_series.max().max()} Will be corrected to 1.")
                self.time_series = self.time_series.clip(upper=1)
            self.historical_data_regions.to_csv(
                os.path.join(self.nestor_ee_scenario_path, "historical.csv"))
            self.time_series.fillna(0).to_csv(
                os.path.join(self.nestor_ee_scenario_path, "timeseries.csv"))
            self.potentials.to_csv(
                os.path.join(self.nestor_ee_scenario_path, "potentials.csv"),
                index=False)
            # self.visualize_FLH()
        elif self.technology=="rooftop_pv":
            for region in regions:
                _region_path = os.path.join(
                    self.nestor_ee_scenario_path, f"{naming_conversion_mapping[self.technology]}_{region}")
                group_name = f"{naming_conversion_mapping[self.technology]}_{region}"
                # check regional availability
                self.region_availability[region] = {}
                # availability for potential
                self.region_availability[region]["potential"]= True
                # are existings there?
                _path_to_existing = os.path.join(
                    _region_path, f"existing_{naming_conversion_mapping[self.technology]}_{region}.csv")
                existing = pd.read_csv(_path_to_existing)
               
                
                
                self.region_availability[region]["existing"] =  len(existing) is not 0 #!               
            
                if (self.region_availability[region]["potential"] is False) and (self.region_availability[region]["existing"] is False):
                    print(f"No data for region {region}")
                    continue
            
                group_name = f"{naming_conversion_mapping[self.technology]}_{region}"

                if self.region_availability[region]["potential"]:
                    self.spatial_Tech_aggregation( region, _region_path)
                    self.prepare_historical_data(
                        _region_path, region, group_name, lifetime)
                    self.prepare_time_series(
                        _region_path, region, group_name, weather_year)
                    self.prepare_potentials(_region_path, group_name, region)
                    self.prepare_FLH(_region_path, region, group_name)
            # check and later TODO
            if self.time_series.min().min() < 0:
                print(
                    f"WARNING: Negative values for time-series. Min value {self.time_series.min().min()} Will be corrected to > 0.")
                self.time_series = self.time_series.clip(lower=0)
            if self.time_series.max().max() > 0:
                print(
                    f"WARNING: Values higher 1 for time-series. Max value {self.time_series.max().max()} Will be corrected to 1.")
                self.time_series = self.time_series.clip(upper=1)
            self.historical_data_regions.to_csv(
                os.path.join(self.nestor_ee_scenario_path, "historical.csv"))
            self.time_series.fillna(0).to_csv(
                os.path.join(self.nestor_ee_scenario_path, "timeseries.csv"))
            self.potentials.to_csv(
                os.path.join(self.nestor_ee_scenario_path, "potentials.csv"),
                index=False)
            # self.visualize_FLH()
                  
        else :
            for region in regions:
                _region_path = os.path.join(
                    self.nestor_ee_scenario_path, f"{naming_conversion_mapping[self.technology]}_{region}")
                group_name = f"{naming_conversion_mapping[self.technology]}_{region}"
                # check regional availability
                self.region_availability[region] = {}
                # availability for potential
                if True:
                    with open(os.path.join(_region_path, "report.json")) as f:
                        report = json.load(f)

                    self.region_availability[region]["potential"] = report["Capacity"] is not 0 #!
                else:
                    pass
                # are existings there?
                _path_to_existing = os.path.join(
                    _region_path, f"existing_{naming_conversion_mapping[self.technology]}_{region}.csv")
                existing = pd.read_csv(_path_to_existing)
                self.region_availability[region]["existing"] = len(existing) is not 0 #!
                
                if (self.region_availability[region]["potential"] is False) and (self.region_availability[region]["existing"] is False):
                    print(f"No data for region {region}")
                    continue
            
                group_name = f"{naming_conversion_mapping[self.technology]}_{region}"
                if self.region_availability[region]["potential"]:
                    self.spatial_Tech_aggregation( region, _region_path)
                    self.prepare_historical_data(
                        _region_path, region, group_name, lifetime)
                    self.prepare_time_series(
                        _region_path, region, group_name, weather_year)
                    self.prepare_potentials(_region_path, group_name, region)
                    self.prepare_FLH(_region_path, region, group_name)
        
            # check and later TODO
            if self.time_series.min().min() < 0:
                print(
                    f"WARNING: Negative values for time-series. Min value {self.time_series.min().min()} Will be corrected to > 0.")
                self.time_series = self.time_series.clip(lower=0)
            if self.time_series.max().max() > 0:
                print(
                    f"WARNING: Values higher 1 for time-series. Max value {self.time_series.max().max()} Will be corrected to 1.")
                self.time_series = self.time_series.clip(upper=1)
            self.historical_data_regions.to_csv(
                os.path.join(self.nestor_ee_scenario_path, "historical.csv"))
            self.time_series.to_csv(
                os.path.join(self.nestor_ee_scenario_path, "timeseries.csv"))
            self.potentials.to_csv(
                os.path.join(self.nestor_ee_scenario_path, "potentials.csv"),
                index=False)
        # self.visualize_FLH()

    #preparation of Full Load Hours 
    def prepare_FLH(self, _region_path, region, group_name):
       
        potential_FLH = pd.read_csv(os.path.join(
            _region_path, f"{naming_conversion_mapping[self.technology]}_{region}.csv"))
        existing_FLH = pd.read_csv(os.path.join(
            _region_path, f"existing_{naming_conversion_mapping[self.technology]}_{region}.csv"))
        potential_FLH["FLH"] = 0
        existing_FLH["FLH"] = 0
        if potential_FLH["FLH"].empty:
            potential_FLH["FLH"] = 0
        else:
            pass
        self.fullloadhours[group_name] = potential_FLH["FLH"].to_list()
        self.fullloadhours[group_name+"_stock"] = existing_FLH["FLH"].to_list()
        
        
        # timeseries
        report = os.path.join(_region_path, "report.json")
        if os.path.exists(report):
            try:
                with open(report) as f:
                    report = json.load(f)
                    potential = report["Capacity"]
                    
            except:
                potential = 0
        else:
            potential = pd.read_csv(os.path.join(_region_path,f'{naming_conversion_mapping[self.technology]}_{region}.csv'))["capacity"].sum()/1000000

        energy = pd.read_csv(os.path.join(
            _region_path, f"ts_{naming_conversion_mapping[self.technology]}_{region}_2014.csv"), index_col=0).sum()[0]
        
        self.fullloadhours_stan[group_name] = energy/potential
            
        
    def visualize_FLH(self):
        import matplotlib.pyplot as plt
        data = [self.fullloadhours[i] for i in self.fullloadhours]
        names = [i for i in self.fullloadhours]
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.set_ylabel('Full load hours [h]')
        ax.boxplot(data)
        ax.tick_params(direction='inout')
        ax.set_yticks([x for x in range(0, 8000, 1000)], minor=False)
        ax.set_yticks([x for x in range(500, 8000, 1000)], minor=True)
        ax.set_xticklabels(names, rotation=45, horizontalalignment='right')
        ax.yaxis.grid(True, which='major')
        ax.yaxis.grid(True, which='minor')
        ax.set_ylim(0)
        plt.tight_layout()
        plt.savefig(os.path.join(self.nestor_ee_scenario_path, "FLH.png"))

        # other naming
        clearnames = []
        for name in names:
            _cn = name.replace(f"{naming_conversion_mapping[self.technology]}_", "")
            for fs_number, fs_state in FEDERAL_STATE_NAMING.items():
                if fs_number in _cn:
                    _cn = _cn.replace(fs_number, fs_state)
            clearnames.append(_cn)
        ax.set_xticklabels(clearnames, rotation=45,
                        horizontalalignment='right')
        plt.tight_layout()
        plt.savefig(os.path.join(
            self.nestor_ee_scenario_path, "FLH_clearnames.png"))

    def prepare_time_series(self, path, region, group_name, weather_year):
        # TODO add info of time_series to final data!
        
        timeseries = pd.read_csv(os.path.join(
            path, f"ts_{naming_conversion_mapping[self.technology]}_{region}_{weather_year}.csv"), index_col=0)
        
        timeseries_existing = pd.read_csv(os.path.join(
            path, f"ts_existing_{naming_conversion_mapping[self.technology]}_{region}_{weather_year}.csv"), index_col=0)
    
        existing_capacity = pd.read_csv(os.path.join(
            path, f"existing_{naming_conversion_mapping[self.technology]}_{region}.csv"), index_col=0)["capacity"].sum()
        if os.path.exists(os.path.join(
            path, f"{naming_conversion_mapping[self.technology]}_{region}.csv")):
               
                potential_capacity = pd.read_csv(os.path.join(
                    path, f"{naming_conversion_mapping[self.technology]}_{region}.csv"), index_col=0)
                if 'capacity' in potential_capacity.columns:
                    potential_capacity = potential_capacity["capacity"].sum()
                else:
                    potential_capacity = 0
               
       
        
    
        if len(timeseries.columns) > 32:
            print(timeseries.columns)
            raise NotImplementedError(
                "Currently only one time-series per region can be passed")
        if len(timeseries_existing.columns) > 36:
            raise NotImplementedError(
                "Currently only one time-series per region can be passed")

        self.time_series[group_name] = \
            round(timeseries.iloc[:, 0]/potential_capacity, 4)
        self.time_series[group_name + "_stock"] = \
            round(timeseries_existing.iloc[:, 0]/existing_capacity, 4)
        if self.region_availability[region]["potential"]:
            timeseries = pd.read_csv(os.path.join(
                path, f"ts_{naming_conversion_mapping[self.technology]}_{region}_{weather_year}.csv"), index_col=0)
            if len(timeseries.columns) > 32:
                raise NotImplementedError(
                    "Currently only one time-series per region can be passed")
            
            self.time_series[group_name] = \
                round(timeseries.iloc[:, 0]/potential_capacity, 4)

        if self.region_availability[region]["existing"]:
            timeseries_existing = pd.read_csv(os.path.join(
                path, f"ts_existing_{naming_conversion_mapping[self.technology]}_{region}_{weather_year}.csv"), index_col=0)
            if len(timeseries_existing.columns) > 32:
                raise NotImplementedError(
                    "Currently only one time-series per region can be passed")
            existing_capacity = pd.read_csv(os.path.join(
                path, f"existing_{naming_conversion_mapping[self.technology]}_{region}.csv"), index_col=0)["capacity"].sum()
            self.time_series[group_name + "_stock"] = \
                round(timeseries_existing.iloc[:, 0]/existing_capacity, 4)
           

           
        
    def prepare_historical_data(self, path, region, group_name, lifetime):
        
        if self.technology=="rooftop_pv":

            # db_path = r"R:\data\s-risch\shared_datasources\mastr\mastr_20220721.db"
            db_path = r"/storage/internal/data/s-risch/shared_datasources/mastr/mastr_20220721.db"
            query = f"SELECT ENH_Bundesland, ENH_Nettonennleistung, ENH_HauptAusrichtung, ENH_HauptNeigungswinkel, ENH_InbetriebnahmeDatum FROM processed WHERE ENH_Lage='Bauliche Anlagen (Hausdach, Gebäude und Fassade)' and ENH_Bundesland='{FEDERAL_STATE_NAMING[region]}'"
            # raw_wts = db_query(query)
            engine = create_engine(
                "sqlite:///" + db_path + "/?charset=utf8mb4")
            raw_wts = pd.read_sql(sql=query, con=engine)

            # filter out rows with nan values
            before_capacity = raw_wts["ENH_Nettonennleistung"].sum()
            raw_wts = raw_wts[raw_wts["ENH_HauptAusrichtung"] != None]
            raw_wts = raw_wts[raw_wts["ENH_HauptNeigungswinkel"] != None]
            raw_wts = raw_wts[raw_wts["ENH_HauptNeigungswinkel"]
                            != "Fassadenintegriert"]
            raw_wts = raw_wts[raw_wts["ENH_HauptNeigungswinkel"]
                            != "Nachgeführt"]
            raw_wts = raw_wts[raw_wts["ENH_InbetriebnahmeDatum"] != None]
            raw_wts = raw_wts[~raw_wts["ENH_InbetriebnahmeDatum"].isnull()]
            after_capacity = raw_wts["ENH_Nettonennleistung"].sum()
            print(
                f"Filtering uncomplete data drops '{before_capacity-after_capacity}' capacty")

            # check if there are orientation with are later not mapped
            known_orientation = \
                ["Nord", "Nord-Ost", "Nord-West", "Ost-West",
                "West", "Ost", "Süd-West", "Süd-Ost", "Süd", "Ost-West", None]
            unknown_orientation = [
                x for x in raw_wts["ENH_HauptAusrichtung"].unique() if x not in known_orientation]
            if len(unknown_orientation) > 1:
                print(unknown_orientation)
                raise ValueError()

            # map the orientation to trep names
            raw_wts.loc[raw_wts["ENH_HauptAusrichtung"]
                        == "Nord", "group"] = "N"  # N=0c
            raw_wts.loc[raw_wts["ENH_HauptAusrichtung"]
                        == "Nord-Ost", "group"] = "NE"  # NE=45°
            raw_wts.loc[raw_wts["ENH_HauptAusrichtung"]
                        == "Ost", "group"] = "E"  # E=90°
            raw_wts.loc[raw_wts["ENH_HauptAusrichtung"]
                        == "Süd-Ost", "group"] = "SE"  # SE=135°
            raw_wts.loc[raw_wts["ENH_HauptAusrichtung"]
                        == "Süd", "group"] = "S"  # S=180°
            raw_wts.loc[raw_wts["ENH_HauptAusrichtung"]
                        == "Süd-West", "group"] = "SW"  # SW=225°
            raw_wts.loc[raw_wts["ENH_HauptAusrichtung"]
                        == "West", "group"] = "W"  # W=270°
            raw_wts.loc[raw_wts["ENH_HauptAusrichtung"]
                        == "Nord-West", "group"] = "NW"  # NW# =315°
            # TODO for later - how to deal with East-West
            raw_wts.loc[raw_wts["ENH_HauptAusrichtung"]
                        == "Ost-West", "group"] = "S"

            # check if there are tilts with are later not mapped
            known_tilt = \
                ["< 20 Grad", "20 - 40 Grad", "40 - 60 Grad",
                "> 60 Grad", None]
            unknown_tilt = [
                x for x in raw_wts["ENH_HauptNeigungswinkel"].unique() if x not in known_tilt]
            if len(unknown_tilt) > 1:
                print(unknown_tilt)
                raise ValueError()

            # map the tilts to trep names
            raw_wts.loc[raw_wts["ENH_HauptNeigungswinkel"]
                        == "< 20 Grad", "group"] += "1"  # tilt < 20°
            raw_wts.loc[raw_wts["ENH_HauptNeigungswinkel"]
                        == "20 - 40 Grad", "group"] += "2"  # tilt 20 - 40°
            raw_wts.loc[raw_wts["ENH_HauptNeigungswinkel"]
                        == "40 - 60 Grad", "group"] += "3"  # tilt 40-60 °
            raw_wts.loc[raw_wts["ENH_HauptNeigungswinkel"]
                        == "> 60 Grad", "group"] += "4"  # tilt >= 60°

            # rename and get capacity per group
            raw_wts["commissioning_year"] = \
                raw_wts.apply(
                lambda x: int(datetime.fromisoformat(
                    x["ENH_InbetriebnahmeDatum"]).year) if isinstance(x["ENH_InbetriebnahmeDatum"], str) else x["ENH_InbetriebnahmeDatum"],
                axis=1)
            raw_wts["commissioning_year"] = [int(x)
                                            for x in raw_wts["commissioning_year"]]

            # rename, cleaning and grouping
            raw_wts = raw_wts.rename({"ENH_Nettonennleistung}": "capacity"})
            raw_wts = raw_wts.drop(columns=["ENH_Bundesland",
                                            "ENH_HauptAusrichtung", "ENH_HauptNeigungswinkel", "ENH_InbetriebnahmeDatum"])
            
            raw_wts.columns =('RooftopPV capacity','group', 'commissioning_year')
            
            commissioning_per_year = raw_wts.groupby(["commissioning_year"]).sum()['RooftopPV capacity']
            print(commissioning_per_year)
            
              
            if commissioning_per_year.index[0] > 1989:
                first_index = 1989
            else:
                first_index = commissioning_per_year.index[0]

            commissioning_per_year = \
                commissioning_per_year.reindex(range(first_index, 2051)).fillna(0)

            # set up historical template
            years = ["<1990"]
            years.extend(range(1990, 2051))
            historical_stock = pd.Series(index=years, dtype='float64').fillna(0)
            # <1990
            historical_stock["<1990"] = \
                commissioning_per_year[commissioning_per_year.index <
                                    1990].sum().copy()
            # up to 2020
            historical_stock.loc[1990:2020] += \
                commissioning_per_year.cumsum().loc[1990:2020]
            historical_stock = historical_stock.fillna(0)
           

            # for 2021
            historical_stock[2021] = \
                historical_stock[2021-1] + commissioning_per_year[2021] - \
                commissioning_per_year.loc[:2021-lifetime].sum()
            # append decommissioning from 2020
            for year in range(2022, 2051):
                # existing + comissioned + decomissioning
                historical_stock[year] = \
                    historical_stock[year-1] + commissioning_per_year[year] - \
                    commissioning_per_year.loc[year-lifetime]

            self.historical_data_regions[group_name] = historical_stock/1000000
            

        else:         
            existing = pd.read_csv(os.path.join(
                path, f"existing_{naming_conversion_mapping[self.technology]}_{region}.csv"), index_col=0)
            commissioning_per_year = existing.groupby(
                "commissioning_year").sum()["capacity"]
           
            
            if commissioning_per_year.index[0] > 1989:
                first_index = 1989
            else:
                first_index = commissioning_per_year.index[0]

            commissioning_per_year = \
                commissioning_per_year.reindex(range(first_index, 2051)).fillna(0)

            # set up historical template
            years = ["<1990"]
            years.extend(range(1990, 2051))
            historical_stock = pd.Series(index=years, dtype='float64').fillna(0)
            # <1990
            historical_stock["<1990"] = \
                commissioning_per_year[commissioning_per_year.index <
                                    1990].sum().copy()
            # up to 2020
            historical_stock.loc[1990:2020] += \
                commissioning_per_year.cumsum().loc[1990:2020]
            historical_stock = historical_stock.fillna(0)
           

            # for 2021
            historical_stock[2021] = \
                historical_stock[2021-1] + commissioning_per_year[2021] - \
                commissioning_per_year.loc[:2021-lifetime].sum()
            # append decommissioning from 2020
            for year in range(2022, 2051):
                # existing + comissioned + decomissioning
                historical_stock[year] = \
                    historical_stock[year-1] + commissioning_per_year[year] - \
                    commissioning_per_year.loc[year-lifetime]

            self.historical_data_regions[group_name] = historical_stock/1000000

             

    def prepare_potentials(self, path, group_name, region):
        
        report_path = os.path.join(path, "report.json")
        if os.path.exists(report_path):
            try:
                with open(report_path) as f:
                    report = json.load(f)
                # potential capacity is stock_capacity plus additional capacity
                # TODO currently hard_coded to 2020 -> needs refyear actually!
                capacity = round(
                    (report["Capacity"]/1000000 +
                        self.historical_data_regions.loc[2020, group_name]), 4)
            except:
                capacity = 0
        else:
            capacity = pd.read_csv(os.path.join(path,f'{naming_conversion_mapping[self.technology]}_{region}.csv'))["capacity"].sum()/1000000 + self.historical_data_regions.loc[:, group_name].max()

            # potential
        if self.region_availability[region]["potential"]:
            
            new_row = pd.DataFrame(
                {"name": [group_name], "profile": [group_name],
                "lb": [0], "ub": [capacity]})
            self.potentials = pd.concat([self.potentials, new_row], axis=0)

            # existing
        if self.region_availability[region]["existing"]:
        
            new_row_existing = pd.DataFrame(
                {"name": [group_name+"_stock"], "profile": [group_name+"_stock"],
                "lb": [0], "ub": [0]})
            self.potentials = pd.concat(
                [self.potentials, new_row_existing], axis=0)

    def exclude_existing(self):

        if self.technology == 'offshore':
            rs_list=exclusive_economic_zones
        else:
            rs_list = FEDERAL_STATES
  
        with Pool(len(rs_list)) as p:
            res = p.map(self._exclude_existing, rs_list) 
        for r in res:
            print(r)

    def rooftop_potentials(self):
        
        with Pool(len(FEDERAL_STATES)) as p:
            res = p.map(self._rooftop_potentials, FEDERAL_STATES) 
        for r in res:
            print(r)

    def _rooftop_potentials(self, rs):
        new_case_name=None

        from trep import TREP
        import time

        path = glob.glob('/storage/internal/data/s-risch/db_TREP/rooftop_nuga_22_07_04/RooftopPV_*')


        #for rs in FEDERAL_STATES:
        print(f"\n\nRegion: {int(rs)}")
        print("New case name: '{}'".format(new_case_name))
        
                
        existing = []
        potential = []
        ts_existing = []
        ts_potential = []
        municipality_list = []
        
        column = ['E1', 'S3', 'SE2', 'SW1', 'SE1', 'S2', 'E3', 'NW1', 'S1', 'E4', 'SE3','SW2', 'SW4', 'W2', 'S4', 'E2', 'N1', 'SW3', 'W3', 'NW2', 'W1', 'NE1','NE2', 'NW4', 'NW3', 'N3', 'W4', 'SE4', 'N2', 'N4', 'NE3', 'NE4']
        
        
        for a in sorted(path):
            #print(a)
            code = os.path.basename(a).split('_')[1]
            if code.startswith(rs):
                print(f'process region:{rs}')
                municipality= code
                print(municipality)
                start = time.time()
                trp = TREP(
                    municipality, level="MUN", case=self.case_name, pixelRes=10,
                    use_intermediate=True, db_path="CAESAR",
                    intermediate_path="CAESAR", datasource_path="CAESAR",
                    dlm_basis_path="CAESAR", hu_path="CAESAR")
                
                if new_case_name is None:
                        nestor_ee_result_path = os.path.join(
                        self.nestor_ee_db_path, self.case_name, f'RooftopPV_{rs}')
                else:
                    nestor_ee_result_path = os.path.join(
                        self.nestor_ee_db_path, new_case_name, f'RooftopPV_{rs}')
                print("nestor_ee_result_path")
                print(nestor_ee_result_path)
                if not os.path.isdir(nestor_ee_result_path):
                    os.makedirs(nestor_ee_result_path)

                if all([os.path.isfile(os.path.join(nestor_ee_result_path, f)) for f in [f"RooftopPV_{rs}.csv", "report.json", "RooftopPV_potential_area.tif", "RooftopPV_potential_items.shp"]]):
                    print("All files here")
                    pass  
                else:
                    #trp.RooftopPV._load_eligible_area()
                    trp.check_existing_db("RooftopPV")
                    trp.RooftopPV.estimate_potential(
                        deduct_existing=False
                        )
                    
                trp.RooftopPV.sim()
                trp.RooftopPV.predicted_items
                trp.RooftopPV.ts_existing_items = trp.RooftopPV.ts_existing_items.reindex(columns=column)
                trp.RooftopPV.ts_predicted_items = trp.RooftopPV.ts_predicted_items.reindex(columns=column)
                
                existing.append(trp.RooftopPV.existing_items)
                

                # trp.RooftopPV.sim(**{"n_groups": 1})
                trp.RooftopPV.sim_existing()
                potential.append(trp.RooftopPV.predicted_items)
                ts_existing.append(trp.RooftopPV.ts_existing_items)
                ts_potential.append(trp.RooftopPV.ts_predicted_items)
                if new_case_name is not None:
                    trp.case = new_case_name
                trp.db_path = self.nestor_ee_db_path
                trp.RooftopPV.result_path = nestor_ee_result_path
                # print(trp.RooftopPV.result_path)
                # trp.to_db("RooftopPV")
                # trp.existing_to_db("RooftopPV")
                
                rs2 = "{:02d}".format(2)
                print(rs2)
                
                municipality_list.append(municipality)
                print(
                "Estimated potential after "
                + f"{(time.time() - start) / 60} minutes",
                flush=True,
                )
                                    
                print('list in mun: \n', municipality_list)
            
        if rs=='0o2' or rs==11:
                trp.RooftopPV.predicted_items.to_csv(nestor_ee_result_path+f'/RooftopPV_{rs}.csv')       
        try:
        
            df_existing =reduce(lambda x, y: x.add(y, fill_value=0), existing)
            df_potential =reduce(lambda x, y: x.add(y, fill_value=0), potential)
            df_ts_existing =reduce(lambda x, y: x.add(y, fill_value=0), ts_existing)
            df_ts_potential =reduce(lambda x, y: x.add(y, fill_value=0), ts_potential)

            print(f'total existing data in region {rs}' , df_existing )
            print(f'total potential data in region {rs}' , df_potential )
            print(f'total existing timeseries data in region {rs}' , df_ts_existing )
            print(f'total existing timeseries datain region {rs}' , df_ts_potential )
                
            df_existing.to_csv(nestor_ee_result_path+f'/existing_RooftopPV_{rs}.csv')
            df_potential.to_csv(nestor_ee_result_path+f'/RooftopPV_{rs}.csv')
            df_ts_existing.to_csv(nestor_ee_result_path+f'/ts_existing_RooftopPV_{rs}_2014.csv')
            df_ts_potential.to_csv(nestor_ee_result_path+f'/ts_RooftopPV_{rs}_2014.csv')
        except Exception as e:
            print(f'cannot solve due to {e} in region {rs}')
        
            
          
           
        print('end in region', rs)
        
    def spatial_Tech_aggregation(self, region, _region_path):
        from trep import Technology
        placements = pd.read_csv(os.path.join(_region_path,f'existing_{naming_conversion_mapping[self.technology]}_{region}.csv'))
        
        if self.technology=='offshore' or self.technology=='onshore':
        
          Technology.sim_wind(placements,
                                grouping_method="spagat",
                                n_groups=7,
                                turbine=None,
                                year=2014,
                                offshore=False,
                                **grouping_kwargs,
                            )
          print(Technology.sim_wind(placements,
                                grouping_method="spagat",
                                n_groups=7,
                                turbine=None,
                                year=2014,
                                offshore=False,
                                **grouping_kwargs,
                            ))
        elif self.technology.endswith("pv"): 
          Technology.sim_pv(
                            placements,
                            grouping_method="spagat",
                            n_groups=1,
                            module="LG Electronics LG370Q1C-A5",
                            poa_bound=0,
                            grouping=True,
                            year=2014,
                            workflow="ERA5",
                        )
    
    def _exclude_existing(self, rs, *argv,**kwargs): #technology
        new_case_name=None
        
        from trep import TREP
        import time

        if self.technology == 'offshore':
            rs_list=exclusive_economic_zones
        else:
            rs_list = FEDERAL_STATES
       
       
        if self.technology == 'onshore':
            turbine="SG4.7-155"  
            hub_height=120.5 
               
            print(f"\n\nRegion: {rs}")
            print("New case name: '{}'".format(new_case_name))
            print("Turbine {}".format(turbine))
            print("hub_height {}".format(hub_height))

            start = time.time()
            trp = TREP(
                rs, level="state", case=self.case_name, pixelRes=10,
                use_intermediate=True, db_path="CAESAR",
                intermediate_path="CAESAR", datasource_path="CAESAR",
                dlm_basis_path="CAESAR", hu_path="CAESAR")

            # check if existing, where already excluded
            report_path = os.path.join(trp.Wind.result_path, "report.json")
            with open(report_path) as f:
                report = json.load(f)

            if "existing" in report.keys():
                if report["existing"] is not None:
                    raise ValueError(
                        "Chosen TREP_DB case already excluded existing technologies.")
            if new_case_name is None:
                nestor_ee_result_path = os.path.join(
                    self.nestor_ee_db_path, self.case_name, f'Wind_{rs}')
            else:
                nestor_ee_result_path = os.path.join(
                    self.nestor_ee_db_path, new_case_name, f'Wind_{rs}')
            print("nestor_ee_result_path")
            print(nestor_ee_result_path)
            if not os.path.isdir(nestor_ee_result_path):
                os.makedirs(nestor_ee_result_path)

            if all([os.path.isfile(os.path.join(nestor_ee_result_path, f)) for f in [f"Wind_{rs}.csv", "report.json", "Wind_potential_area.tif", "Wind_potential_items.shp"]]):
                print("All files here")
                pass
            else:
                trp.Wind.load_turbine_from_library(turbine, hub_height)
                trp.Wind._load_eligible_area()
                trp.Wind.estimate_potential(
                    update=False,
                    exclusion_dict= {"existing": {                       
                        "distance": [8,4], "wind_dir": "from_era", "target_diameter": 132}},
                            # "existing": {
                    # {   "target_diameter": trp.Wind.target_diameter}},
                    ignore_db=False, predict=True, rerun=True )

                trp.Wind.turbine = None
                if "powerCurve" in trp.Wind.predicted_items.columns:
                    trp.Wind.predicted_items = trp.Wind.predicted_items.drop(
                        "powerCurve", axis=1)

                trp.Wind.sim(**{"n_groups": 1})
                trp.Wind.sim_existing()
                if new_case_name is not None:
                    trp.case = new_case_name
                trp.db_path = self.nestor_ee_db_path
                trp.Wind.result_path = nestor_ee_result_path
                trp.to_db("Wind")
                trp.existing_to_db("Wind")

            print( trp.Wind.result_path)
            print(trp.db_path)
            print(
                "Estimated potential after "
                + f"{(time.time() - start) / 60} minutes",
                flush=True,
            )
                
        elif self.technology == 'offshore': 
            turbine="SG4.7-155"  
            hub_height=120.5 
            #turbine=kwargs['turbine']
            #hub_height=kwargs['hub_height'])
            
            print(f"\n\nRegion: {rs}")
            print("New case name: '{}'".format(new_case_name))
            print("Turbine {}".format(turbine))
            print("hub_height {}".format(hub_height))

        
            start = time.time()
            
            if "EEZ" in rs:
                rs=rs.replace("_EEZ","")
                _level='eez'
            else: 
                _level="state"
        
            trp = TREP(
                rs, level=_level, case=self.case_name, pixelRes=10,
                use_intermediate=True, db_path="CAESAR",
                intermediate_path="CAESAR", datasource_path="CAESAR",
                dlm_basis_path="CAESAR", hu_path="CAESAR")
                    #     print(e)
        
            # check if existing, where already excluded
            report_path = os.path.join(trp.WindOffshore.result_path, "report.json")
            with open(report_path) as f:
                report = json.load(f)

            if "existing" in report.keys():
                if report["existing"] is not None:
                    raise ValueError(
                        "Chosen TREP_DB case already excluded existing technologies.")
            # create folder for region
            if _level == "eez":
                region_str=rs+"_EEZ"
            else: 
                region_str=rs
            if new_case_name is None:
                nestor_ee_result_path = os.path.join(
                    self.nestor_ee_db_path, self.case_name, f'WindOffshore_{region_str}')
            else:
                nestor_ee_result_path = os.path.join(
                    self.nestor_ee_db_path, new_case_name, f'WindOffshore_{region_str}')
            print("nestor_ee_result_path")
            print(nestor_ee_result_path)
            if not os.path.isdir(nestor_ee_result_path):
                os.makedirs(nestor_ee_result_path)

            if all([os.path.isfile(os.path.join(nestor_ee_result_path, f)) for f in [f"WindOffshore_{rs}.csv", "report.json", "WindOffshore_potential_area.tif", "WindOffshore_potential_items.shp"]]):
                print("All files here")
                pass
            else:
                trp.WindOffshore.load_turbine_from_library(turbine_name=kwargs['turbine'],hub_height=kwargs['hub_height'])
                trp.WindOffshore._load_eligible_area()
                trp.WindOffshore.estimate_potential(
                    update=False,
                    exclusion_dict= {"existing": {
                        "distance": [8,4], "wind_dir": "from_era", "target_diameter": 132}},
                    ignore_db=False, predict=True, rerun=True )

                trp.WindOffshore.turbine = None
                if "powerCurve" in trp.WindOffshore.predicted_items.columns:
                    trp.WindOffshore.predicted_items = trp.WindOffshore.predicted_items.drop(
                        "powerCurve", axis=1)

                trp.WindOffshore.sim(**{"n_groups": 1})
                trp.WindOffshore.sim_existing()
                if new_case_name is not None:
                    trp.case = new_case_name
                trp.db_path = self.nestor_ee_db_path
                trp.WindOffshore.result_path = nestor_ee_result_path
                trp.to_db("WindOffshore")
                trp.existing_to_db("WindOffshore")

            print(trp.WindOffshore.result_path)
            print(
                "Estimated potential after "
                + f"{(time.time() - start) / 60} minutes",
                flush=True,
            )
                
        elif self.technology == 'openfield_pv': 
        
            print(f"\n\nRegion: {rs}")
            print("New case name: '{}'".format(new_case_name))

            start = time.time()
            trp = TREP(
                rs, level="state", case=self.case_name, pixelRes=10,
                use_intermediate=True, db_path="CAESAR",
                intermediate_path="CAESAR", datasource_path="CAESAR",
                dlm_basis_path="CAESAR", hu_path="CAESAR")

            # check if existing, where already excluded
            report_path = os.path.join(trp.OpenfieldPV.result_path, "report.json")
            with open(report_path) as f:
                report = json.load(f)

            if "existing" in report.keys():
                if report["existing"] is not None:
                    raise ValueError(
                        "Chosen TREP_DB case already excluded existing technologies.")
            if new_case_name is None:
                nestor_ee_result_path = os.path.join(
                    self.nestor_ee_db_path, self.case_name, f'OpenfieldPV_{rs}')
            else:
                nestor_ee_result_path = os.path.join(
                    self.nestor_ee_db_path, new_case_name, f'OpenfieldPV_{rs}')
            print("nestor_ee_result_path")
            print(nestor_ee_result_path)
            if not os.path.isdir(nestor_ee_result_path):
                os.makedirs(nestor_ee_result_path)

            if all([os.path.isfile(os.path.join(nestor_ee_result_path, f)) for f in [f"OpenfieldPV_{rs}.csv", "report.json", "OpenfieldPV_potential_area.tif", "OpenfieldPV_potential_items.shp"]]):
                print("All files here")
                pass
            else:
                trp.OpenfieldPV._load_eligible_area()
                trp.OpenfieldPV.estimate_potential(
                    update=False,
                    exclusion_dict={"existing": {
                        "ExclusionCriteria": trp.OpenfieldPV.ec}},
                    ignore_db=False, predict=True, rerun=True )
                # columns = [capacity, lat, lon, azimuth, tilt, share, ID, generation, poa]

           

                trp.OpenfieldPV.sim(**{"n_groups": 1})
                trp.OpenfieldPV.sim_existing()
                print(f'predicted items {rs} ts:',trp.OpenfieldPV.ts_predicted_items.columns)
                
                trp.OpenfieldPV.existing_items.to_csv(nestor_ee_result_path+f'/existing_OpenfieldPV_{rs}.csv')
                trp.OpenfieldPV.predicted_items.to_csv(nestor_ee_result_path+f'/OpenfieldPV_{rs}.csv')
                trp.OpenfieldPV.ts_existing_items.to_csv(nestor_ee_result_path+f'/ts_existing_OpenfieldPV_{rs}_2014.csv')
                trp.OpenfieldPV.ts_predicted_items.to_csv(nestor_ee_result_path+f'/ts_OpenfieldPV_{rs}_2014.csv')
                if new_case_name is not None:
                    trp.case = new_case_name
                trp.db_path = self.nestor_ee_db_path
                trp.OpenfieldPV.result_path = nestor_ee_result_path
                trp.to_db("OpenfieldPV")
                trp.existing_to_db("OpenfieldPV")

            print(trp.OpenfieldPV.result_path)
            print(
                "Estimated potential after "
                + f"{(time.time() - start) / 60} minutes",
                flush=True,
            )
    
        else:
            pass

