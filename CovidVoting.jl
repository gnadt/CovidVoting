## comparing covid deaths by 2020 presidential voting, checking results of:
# https://icemsg.wordpress.com/politics/the-republican-democrat-covid-divide/
cd(@__DIR__)
using Pkg; Pkg.activate("."); Pkg.instantiate()
using DataFrames, DelimitedFiles, Plots

#! population section ========================================================
#! no longer used, but keeping just in case

##* load data (unsure where file came from originally, but matches covid data)
file_p = "data/fips_population.txt"
df_p   = DataFrame(readdlm(file_p,',',skipstart=1),:auto);

##* rename columns
rename!(df_p, :x1 => :fips);
rename!(df_p, :x2 => :pop );

##* change data types
df_p[!,:fips] = convert.(Int64,df_p[!,:fips]);
df_p[!,:pop ] = convert.(Int64,df_p[!,:pop ]);

##* add row for Oglala Lakota
push!(df_p, (46102, 14177));

#! covid section =============================================================

##* load data (from https://github.com/CSSEGISandData/COVID-19)
file_c = "data/time_series_covid19_deaths_US.csv"
df_c   = DataFrame(readdlm(file_c,',',skipstart=1),:auto);
df_c   = df_c[df_c[:,:x8].=="US",:]; # only keep US rows
cols   = DataFrame(readdlm(file_c,','),:auto)[1,13:end];

##* select & rename columns
df_c = df_c[:,[5;6;7;12;13:end]]
rename!(df_c, :x5  => :fips)
rename!(df_c, :x6  => :county)
rename!(df_c, :x7  => :state)
rename!(df_c, :x12 => :pop)
for (i,col) in enumerate(names(df_c)[5:end])
    rename!(df_c, col => cols[i])
end

##* remove rows with empty data
for i = 1:4
    df_c = df_c[df_c[:,i].!="",:]
end

##* change data types
df_c[!,:fips  ] = convert.(Int64  ,df_c[!,:fips  ])
df_c[!,:county] = convert.(String ,df_c[!,:county])
df_c[!,:state ] = convert.(String ,df_c[!,:state ])
df_c[!,:pop   ] = convert.(Int64  ,df_c[!,:pop   ])
for col in names(df_c)[5:end]
    df_c[!,col] = convert.(Int64  ,df_c[!,col    ])
end

##* make strings title case & get valid fips
df_c[:,:county] = titlecase.(df_c[:,:county]);
df_c[:,:state ] = titlecase.(df_c[:,:state ]);
df_c = df_c[df_c[:,:fips].<=56045,:];

#! voting section ============================================================

##* load data (from https://doi.org/10.7910/DVN/VOQCHQ)
file_v = "data/countypres_2000-2020.csv"
df_v = DataFrame(readdlm(file_v,',',skipstart=1),:auto);
df_v = df_v[df_v[:,:x1].==2020,:]; # only keep 2020 rows

##* select & rename columns
select!(df_v, [:x5, :x4, :x2, :x8, :x9, :x10, :x12]);
rename!(df_v, :x5  => :fips);
rename!(df_v, :x4  => :county);
rename!(df_v, :x2  => :state);
rename!(df_v, :x8  => :party);
rename!(df_v, :x9  => :votes);
rename!(df_v, :x10 => :total);
rename!(df_v, :x12 => :mode);

##* fix fips values for District of Columbia & Oglala Lakota
df_v[df_v[:,:state ].=="DISTRICT OF COLUMBIA",:fips] .= 11001;
df_v[df_v[:,:county].=="OGLALA LAKOTA",:fips] .= 46102;

##* remove empty & NA rows, except total column
for i = 1:size(df_v,2)
    df_v = df_v[df_v[:,i].!="",:]
    (names(df_v)[i] != "total") && (df_v = df_v[df_v[:,i].!="NA",:])
end

##* fill in total column if needed
fips = unique(df_v[df_v[:,:total].=="NA",:fips])
for i = 1:length(fips)
    total = sum(df_v[df_v[:,:fips].==fips[i],:votes])
    df_v[df_v[:,:fips].==fips[i],:total] .= total
end

##* change data types
df_v[!,:state ] = convert.(String,df_v[!,:state ]);
df_v[!,:county] = convert.(String,df_v[!,:county]);
df_v[!,:fips  ] = convert.(Int64 ,df_v[!,:fips  ]);
df_v[!,:party ] = convert.(String,df_v[!,:party ]);
df_v[!,:votes ] = convert.(Int64 ,df_v[!,:votes ]);
df_v[!,:total ] = convert.(Int64 ,df_v[!,:total ]);
df_v[!,:mode  ] = convert.(String,df_v[!,:mode  ]);

##* make strings title case & get valid fips
df_v[:,:county] = titlecase.(df_v[:,:county]);
df_v[:,:state ] = titlecase.(df_v[:,:state ]);
df_v[:,:party ] = titlecase.(df_v[:,:party ]);
df_v[:,:mode  ] = titlecase.(df_v[:,:mode  ]);
df_v = df_v[df_v[:,:fips].<=56045,:];

##* add Total rows where needed
for fips in unique(df_v[:,:fips])
    df_vv = df_v[df_v[:,:fips].==fips,:]
    for party in unique(df_vv[:,:party])
        votes = sum(df_vv[df_vv[:,:party].==party,:votes])
        push!(df_v, (df_vv[1,:fips], df_vv[1,:county], df_vv[1,:state],
                     party,   votes, df_vv[1,:total ], "Total"))
    end
end
df_v = df_v[df_v[:,:mode].=="Total",:]; # only keep Total rows
select!(df_v, DataFrames.Not(:mode)); # remove mode column now

##* sort & remove duplicates
sort!(df_v);
unique!(df_v);

#! evaluation section ========================================================

##* get fips occuring in both covid data and voting data
fips_c = unique(df_c[:,:fips]);
fips_v = unique(df_v[:,:fips]);
fips  = []
count = 0
for i = 1:length(fips_c)
    if fips_c[i] in fips_v
        push!(fips,fips_c[i])
    else
        count += 1
        # println(fips_c[i])
    end
end
println("fips in covid data but not in voting data: $count")
count = 0
for i = 1:length(fips_v)
    if !(fips_v[i] in fips_c)
        count += 1
        # println(fips_v[i])
    end
end
println("fips in voting data but not in covid data: $count")
println("total usable fips: $(length(fips))")

##* create combined evaluation DataFrame with initial setup row
df_e = DataFrame(fips  = 0,   county = "Null", state   = "Null",
                 pop   = 0,   votes  = 0,
                 dem   = 0,   rep    = 0,      other   = 0, 
                 dem_f = 0.0, rep_f  = 0.0,    other_f = 0.0);

##* create function to get votes for particular party
function get_votes(df::DataFrame, party::String)
    v = df[df[:,:party].==party,:votes]
    if length(v) == 0
        votes = 0
    elseif length(v) == 1
        votes = v[1]
    else
        votes = v[1]
        println("multiple rows of with same party for given fips")
    end
    return (votes)
end # function get_votes

##* fill in combined evaluation DataFrame (everything except deaths)
for i = 1:length(fips)
    df_cc = df_c[df_c[:,:fips].==fips[i],:]
    df_vv = df_v[df_v[:,:fips].==fips[i],:]
    dem   = get_votes(df_vv,"Democrat")
    rep   = get_votes(df_vv,"Republican")
    gre   = get_votes(df_vv,"Green")
    lib   = get_votes(df_vv,"Libertarian")
    other = get_votes(df_vv,"Other") + gre + lib
    total = dem + rep + other
    (total == df_vv.total[1]) || println("total votes do not match")
    push!(df_e, (fips[i],         df_cc[1,:county], df_cc[1,:state],
                 df_cc[1,:pop],   total,
                 dem,             rep,              other,
                 dem/total,       rep/total,        other/total
    ))
end

##* remove initial setup row
df_e = df_e[2:end,:];

##* fill in combined evaluation DataFrame (deaths)
df_e = hcat(df_e,df_c[[fips_c[i] in fips for i = 1:length(fips_c)],5:end]);

##* create bucket DataFrames
b1 = 50
b2 = 60
b3 = 80
df_1 = df_e[ df_e[:,:rep_f] .>= b3/100,:]
df_2 = df_e[(df_e[:,:rep_f] .>= b2/100) .& (df_e[:,:rep_f] .< b3/100),:]
df_3 = df_e[(df_e[:,:rep_f] .>= b1/100) .& (df_e[:,:rep_f] .< b2/100),:]
df_4 = df_e[(df_e[:,:dem_f] .>= b1/100) .& (df_e[:,:dem_f] .< b2/100),:]
df_5 = df_e[(df_e[:,:dem_f] .>= b2/100) .& (df_e[:,:dem_f] .< b3/100),:]
df_6 = df_e[ df_e[:,:dem_f] .>= b3/100,:]
num = size(df_1,1) + size(df_2,1) + size(df_3,1) + 
      size(df_4,1) + size(df_5,1) + size(df_6,1)
println("total counties that fit into buckets: $num of $(length(fips))")

##* create function to get column values for particular bucket DataFrame
function get_buck(df::DataFrame, bin::String, days::Int=14)
    df = df[:,vcat("state","pop",[cols[i] for i = 1:length(cols)])]
    rename!(df, "state" => "bin")
    df[1,"bin"] = bin
    df[1,"pop"] = sum(df[:,"pop"])
    days > size(df,2)-3 && println("limiting days to $(size(df,2)-3)")
    days = min(days,size(df,2)-3)
    (d1,d2) = (3+days,size(df,2))
    sums = [round(Int64,(sum(df[:,i]) - sum(df[:,i-days]))/days) for i = d1:d2]                                   
    df[1,d1:d2] = sums
    return (DataFrame(df[1,[1:2;d1:d2]]))
end # function get_buck in here

##* create and fill in final results DataFrame
days = 14
df_f = get_buck(          df_1,"Rep $b3+%"   ,days);
df_f = vcat(df_f,get_buck(df_2,"Rep $b2-$b3%",days));
df_f = vcat(df_f,get_buck(df_3,"Rep $b1-$b2%",days));
df_f = vcat(df_f,get_buck(df_4,"Dem $b1-$b2%",days));
df_f = vcat(df_f,get_buck(df_5,"Dem $b2-$b3%",days));
df_f = vcat(df_f,get_buck(df_6,"Dem $b3+%"   ,days));

##* plotting results
p1 = plot(ylim=(0,2.5),dpi=100)
for i = 1:size(df_f,1)
    plot!(p1,collect(df_f[i,3:end])./df_f[i,2].*100000,lab=df_f[i,1])
end
display(p1)
# png(p1,"results_here")
