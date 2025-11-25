from collections import Counter

page_loads = 0
button_clicks = Counter()
slider_inputs = []
dropdown_selections = Counter()
text_input_updates = 0
toggle_switch_counts = {"on": 0, "off": 0}

events_per_type = Counter()
events_per_second = Counter()