from lightweight_charts.widgets import StreamlitChart

def render_chart(data, title, width=500, height=400):
    """Lightweight Charts を描画する"""
    chart = StreamlitChart(width=width, height=height, toolbox=True)
    chart.legend(True)
    
    chart.run_script(f"""
        {chart.id}.chart.applyOptions({{
            localization: {{
                dateFormat: 'yyyy-MM-dd'
            }}
        }});
    """)

    chart.topbar.textbox('title', title)
    
    chart.set(data)
    
    ma_colors = {5: 'orange', 25: '#A020F0', 75: '#008000'}
    for ma in [5, 25, 75]:
        ma_name = f'MA{ma}'
        if ma_name in data.columns:
            line = chart.create_line(name=ma_name, color=ma_colors[ma], width=1)
            line.set(data)
            
    chart.load()
