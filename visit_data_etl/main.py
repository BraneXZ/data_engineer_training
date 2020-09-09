from etl import visit_data

if __name__ == '__main__':
    grouped_states = visit_data.extract()
    visit_data.transform(visit_data.in_memory(grouped_states))