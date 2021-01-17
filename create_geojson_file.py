
lines = metro["route_short_name"].unique()
for line in lines:
    df = metro.loc[metro['route_short_name'] == line]
    dfs = dict(tuple(df.groupby('trip_id')))
    features = []
    for key in dfs.keys():
        geometry = [xy for xy in zip(dfs[key].stop_lon, dfs[key].stop_lat,dfs[key].elevation, dfs[key].arrival_time_int)]
        trip = dfs[key].trip_id.unique().tolist()
        insert_features = geojson.Feature(geometry=geojson.LineString(geometry),
                                          properties=dict(trip=dfs[key].trip_id.unique().tolist(),
                                                    stop=dfs[key].stop_id.unique().tolist(),
                                                    route=dfs[key].route_short_name.unique().tolist()
                                                    ))
        features.append(insert_features)

        output_filename = r'<Your Directory>L\{}_new.geojson'.format(line)
        with open(output_filename, 'w', encoding='utf8') as fp:
                geojson.dump(geojson.FeatureCollection(features), fp, sort_keys=True, ensure_ascii=False)
