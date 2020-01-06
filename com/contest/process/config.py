# coding:utf-8
# !/usr/bin/python
# -*- coding:UTF-8 -*-
# set the path-to-files
TRAIN_FILE = "../data_format_label01.csv"
TEST_FILE = "../valid_11_format.csv"

SUB_DIR = "./output"


NUM_SPLITS = 3
RANDOM_SEED = 2017

# types of columns of the dataset dataframe
CATEGORICAL_COLS = [
    # 'ps_ind_02_cat', 'ps_ind_04_cat', 'ps_ind_05_cat',
    # 'ps_car_01_cat', 'ps_car_02_cat', 'ps_car_03_cat',
    # 'ps_car_04_cat', 'ps_car_05_cat', 'ps_car_06_cat',
    # 'ps_car_07_cat', 'ps_car_08_cat', 'ps_car_09_cat',
    # 'ps_car_10_cat', 'ps_car_11_cat',
    'family_name','model_name',
    'phase_1_name','phase_2_name','phase_3_name',
    'phase_4_name','phase_5_name','phase_6_name',
    'phase_7_name','phase_8_name','phase_9_name','phase_10_name'
]

NUMERIC_COLS = [
    # # binary
    # "ps_ind_06_bin", "ps_ind_07_bin", "ps_ind_08_bin",
    # "ps_ind_09_bin", "ps_ind_10_bin", "ps_ind_11_bin",
    # "ps_ind_12_bin", "ps_ind_13_bin", "ps_ind_16_bin",
    # "ps_ind_17_bin", "ps_ind_18_bin",
    # "ps_calc_15_bin", "ps_calc_16_bin", "ps_calc_17_bin",
    # "ps_calc_18_bin", "ps_calc_19_bin", "ps_calc_20_bin",
    # numeric
    "phase_1_label", "phase_2_label", "phase_3_label",
    "phase_4_label", "phase_5_label", "phase_6_label",
    "phase_7_label", "phase_8_label", "phase_9_label", "phase_10_label"
]

IGNORE_COLS = [
    "product_id", "label",
    "phase_1_index", "phase_2_index", "phase_3_index",
    "phase_4_index","phase_5_index", "phase_6_index",
    "phase_7_index", "phase_8_index","phase_9_index", "phase_10_index"
]
