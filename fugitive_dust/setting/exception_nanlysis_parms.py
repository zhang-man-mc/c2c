class EAnalysis:

    # (数据缺失)数据缺失的分钟数
    miss_data_minutes = 45   

    # (数据超低)数据超低，值小于X
    data_low = 0.01

    # (数据长时间无波动) 连续X个值相等
    long_time_no_change = 5

    # (量级突变)连续N个15分钟
    mutation_num = 4

    # (量级突变)变化率超过X
    # 0.5 -> 1.0
    mutation_rate = 1.0

    # (临近超标异常)处于[a,b]
    # 0.8 -> 0.7
    near_exceed_low_value = 0.7

    # (临近超标)处于[a,b]
    near_exceed_high_value = 1

    # (临近超标)次数超过X个
    near_exceed_num = 4

    # (单日超标次数临界）超标次数达X次
    day_exceed_borderline_low_num = 6

    # (单日超标次数临界)但未达到Y次
    Day_exceed_borderline_high_num = 7

    # (变化趋势异常)N个一组
    change_trend_group = 12

    # (变化趋势异常)间隔M
    change_trend_interval = 1

    # (变化趋势异常)平均值相差Y
    # 0.5 -> 1.0
    change_trend_rate = 1.0

    # (超标)  
    exceeding_standard = 1