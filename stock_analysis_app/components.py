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


def render_chart_compact(data, title, height=250):
    """監視用コンパクトチャート。ツールボックス・凡例・スクロール・スケールを無効化し軽量化。"""
    chart = StreamlitChart(width="100%", height=height, toolbox=False)
    chart.legend(False)

    chart.run_script(f"""
        {chart.id}.chart.applyOptions({{
            localization: {{
                dateFormat: 'yyyy-MM-dd'
            }},
            crosshair: {{
                mode: 3
            }},
            handleScroll: false,
            handleScale: false,
            rightPriceScale: {{
                borderVisible: false
            }},
            timeScale: {{
                borderVisible: false,
                fixLeftEdge: true,
                fixRightEdge: true
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
