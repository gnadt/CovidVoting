using DataFrames, DelimitedFiles, Plots

#! population section ========================================================
##* load data
df_p = DataFrame(readdlm("data/fips_population.txt",',',skipstart=1),:auto)

##* rename columns
rename!(df_p, :x1 => :fips)
rename!(df_p, :x2 => :pop )

##* change data types
df_p[!,:fips] = convert.(Int64,df_p[!,:fips])
df_p[!,:pop ] = convert.(Int64,df_p[!,:pop ])

##* add row for Oglala Lakota
push!(df_p, (46102, 13672))

##* sort & remove duplicates
sort!(df_p)
unique!(df_p)

##* get combined Dukes & Nantucket pop for later
dn_pop = df_p[findfirst(df_p[:,:fips].==25007),:pop] + 
         df_p[findfirst(df_p[:,:fips].==25019),:pop]

#! voting section ============================================================

##* load data
df_v = DataFrame(readdlm("data/countypres_2000-2020.csv",',',skipstart=1),:auto)
df_v = df_v[df_v[:,:x1].==2020,:] # only keep 2020 rows

##* select & rename columns
select!(df_v, [:x5, :x4, :x2, :x8, :x9, :x10, :x12])
rename!(df_v, :x5  => :fips)
rename!(df_v, :x4  => :county)
rename!(df_v, :x2  => :state)
rename!(df_v, :x8  => :party)
rename!(df_v, :x9  => :votes)
rename!(df_v, :x10 => :total)
rename!(df_v, :x12 => :mode)

##* fix fips values for District of Columbia & Oglala Lakota
df_v[df_v[:,:state ].=="DISTRICT OF COLUMBIA",:fips] .= 11001
df_v[df_v[:,:county].=="OGLALA LAKOTA",:fips] .= 46102

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
df_v[!,:state ] = convert.(String,df_v[!,:state ])
df_v[!,:county] = convert.(String,df_v[!,:county])
df_v[!,:fips  ] = convert.(Int64 ,df_v[!,:fips  ])
df_v[!,:party ] = convert.(String,df_v[!,:party ])
df_v[!,:votes ] = convert.(Int64 ,df_v[!,:votes ])
df_v[!,:total ] = convert.(Int64 ,df_v[!,:total ])
df_v[!,:mode  ] = convert.(String,df_v[!,:mode  ])

##* make strings title case & get valid fips
df_v[:,:county] = titlecase.(df_v[:,:county])
df_v[:,:state ] = titlecase.(df_v[:,:state ])
df_v[:,:party ] = titlecase.(df_v[:,:party ])
df_v[:,:mode  ] = titlecase.(df_v[:,:mode  ])
df_v = df_v[df_v[:,:fips].<=56045,:]

##* combine Dukes & Nantucket (keeping fips/row for Dukes)
d = "Dukes"
n = "Nantucket"
df_v[df_v[:,:county].==d,:votes]  += df_v[df_v[:,:county].==n,:votes]
df_v[df_v[:,:county].==d,:total]  += df_v[df_v[:,:county].==n,:total]
df_v[df_v[:,:county].==d,:county] .= "Dukes And Nantucket"
df_v = df_v[df_v[:,:county].!=n,:]


##* add Total rows where needed
for fips in unique(df_v[:,:fips])
    df_vv = df_v[df_v[:,:fips].==fips,:]
    for party in unique(df_vv[:,:party])
        votes = sum(df_vv[df_vv[:,:party].==party,:votes])
        push!(df_v, (df_vv[1,:fips], df_vv[1,:county], df_vv[1,:state],
                     party,   votes, df_vv[1,:total ], "Total"))
    end
end
df_v = df_v[df_v[:,:mode].=="Total",:] # only keep Total rows
select!(df_v, DataFrames.Not(:mode)) # remove mode column now

##* add pop column
df_v.pop = zeros(Int64,size(df_v,1))
for i = 1:size(df_v,1)
    j = findfirst(df_p[:,:fips].==df_v[i,:fips])
    (typeof(j) == Nothing) || (df_v[i,:pop] = df_p[j,:pop])
end

##* fix Dukes & Nantucket pop value
df_v[df_v[:,:fips].==25007,:pop] .= dn_pop

##* sort & remove duplicates
sort!(df_v)
unique!(df_v)

#! covid section =============================================================

##* load data
df_c = DataFrame(readdlm("data/09-25-2021.csv",',',skipstart=1),:auto)
df_c = df_c[df_c[:,:x4].=="US",:] # only keep US rows

##* select & rename columns
select!(df_c, [:x1, :x2, :x3, :x8, :x9, :x13])
rename!(df_c, :x1  => :fips)
rename!(df_c, :x2  => :county)
rename!(df_c, :x3  => :state)
rename!(df_c, :x8  => :cases)
rename!(df_c, :x9  => :deaths)
rename!(df_c, :x13 => :c_rate)

##* fix Dukes & Nantucket fips value (keeping fips/row for Dukes)
df_c[df_c[:,:county].=="Dukes and Nantucket",:fips] .= 25007

##* remove rows with empty data
for i = 1:size(df_c,2)
    df_c = df_c[df_c[:,i].!="",:]
end

##* change data types
df_c[!,:fips  ] = convert.(Int64  ,df_c[!,:fips  ])
df_c[!,:county] = convert.(String ,df_c[!,:county])
df_c[!,:state ] = convert.(String ,df_c[!,:state ])
df_c[!,:cases ] = convert.(Int64  ,df_c[!,:cases ])
df_c[!,:deaths] = convert.(Int64  ,df_c[!,:deaths])
df_c[!,:c_rate] = convert.(Float64,df_c[!,:c_rate])

##* make strings title case & get valid fips
df_c[:,:county] = titlecase.(df_c[:,:county])
df_c[:,:state ] = titlecase.(df_c[:,:state ])
df_c = df_c[df_c[:,:fips].<=56045,:]

##* add pop column
# missing some for Alaska, but Alaska voting data doesn't align anyways
# https://twitter.com/jedkolko/status/1321561308546785280
# df_c[:,:cases]./df_c[:,:c_rate]*100000 # to do a check on pop
select!(df_c, DataFrames.Not(:c_rate)) # remove c_rate (was just for pop check)
df_c.pop = zeros(Int64,size(df_c,1))
for i = 1:size(df_c,1)
    j = findfirst(df_p[:,:fips].==df_c[i,:fips])
    (typeof(j) == Nothing) || (df_c[i,:pop] = df_p[j,:pop])
end

##* fix Dukes & Nantucket pop value
df_c[df_c[:,:fips].==25007,:pop] .= dn_pop

##* sort & remove duplicates
sort!(df_c)
unique!(df_c)

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
                 cases = 0,   deaths = 0,
                 pop   = 0,   votes  = 0,
                 dem   = 0,   rep    = 0,      other   = 0, 
                 dem_f = 0.0, rep_f  = 0.0,    other_f = 0.0)

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

##* fill in combined evaluation DataFrame
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
    push!(df_e, (fips[i],         df_vv[1,:county], df_vv[1,:state],
                 df_cc[1,:cases], df_cc[1,:deaths],
                 df_vv[1,:pop],   total,
                 dem,             rep,              other,
                 dem/total,       rep/total,        other/total
    ))
end

##* remove initial setup row
df_e = df_e[2:end,:]

##* create bucket DataFrames
df_1 = df_e[ df_e[:,:rep_f] .>= 0.8,:]
df_2 = df_e[(df_e[:,:rep_f] .>= 0.6) .& (df_e[:,:rep_f] .< 0.8),:]
df_3 = df_e[(df_e[:,:rep_f] .>= 0.5) .& (df_e[:,:rep_f] .< 0.6),:]
df_4 = df_e[(df_e[:,:dem_f] .>= 0.5) .& (df_e[:,:dem_f] .< 0.6),:]
df_5 = df_e[(df_e[:,:dem_f] .>= 0.6) .& (df_e[:,:dem_f] .< 0.8),:]
df_6 = df_e[ df_e[:,:dem_f] .>= 0.8,:]
num = size(df_1,1) + size(df_2,1) + size(df_3,1) + 
      size(df_4,1) + size(df_5,1) + size(df_6,1)
println("total counties that fit into buckets: $num of $(length(fips))")

##* create function to get column values for particular bucket DataFrame
function get_buck(df::DataFrame)
    cases    = sum(df.cases)
    deaths   = sum(df.deaths)
    pop      = sum(df.pop)
    cases_f  = sum(df.cases)/pop
    deaths_f = sum(df.deaths)/pop
    return (cases, deaths, pop, cases_f, deaths_f)
end # function get_buck in here

##* create and fill in final results DataFrame
df_f = DataFrame(bin = "Null", cases   = 0,   deaths   = 0, 
                 pop = 0,      cases_f = 0.0, deaths_f = 0.0)
(cases, deaths, pop, cases_f, deaths_f) = get_buck(df_1)
push!(df_f, ("Rep 80+%"  , cases, deaths, pop, cases_f, deaths_f))
(cases, deaths, pop, cases_f, deaths_f) = get_buck(df_2)
push!(df_f, ("Rep 60-80%", cases, deaths, pop, cases_f, deaths_f))
(cases, deaths, pop, cases_f, deaths_f) = get_buck(df_3)
push!(df_f, ("Rep 50-60%", cases, deaths, pop, cases_f, deaths_f))
(cases, deaths, pop, cases_f, deaths_f) = get_buck(df_4)
push!(df_f, ("Dem 50-60%", cases, deaths, pop, cases_f, deaths_f))
(cases, deaths, pop, cases_f, deaths_f) = get_buck(df_5)
push!(df_f, ("Dem 60-80%", cases, deaths, pop, cases_f, deaths_f))
(cases, deaths, pop, cases_f, deaths_f) = get_buck(df_6)
push!(df_f, ("Dem 80+%"  , cases, deaths, pop, cases_f, deaths_f))

##* remove initial setup row
df_f = df_f[2:end,:]
