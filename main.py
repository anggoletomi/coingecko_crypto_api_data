from cg_data_a_merge_init import cg_data_a_merge_init
from cg_data_c_processed import cg_data_c_processed

from bi_function import log_function

import os
from dotenv import load_dotenv
load_dotenv()

tasks = [
    (cg_data_a_merge_init,{'cg_apikey' : os.getenv("COINGECKO_API_KEY"),
                           'currency' : 'usd',
                           'decimal_precision' : '6',
                           'last_x_days' : 60,
                           'delay_between_request' : 3}),
    (cg_data_c_processed,{}),
    
    ]

log_function(tasks)