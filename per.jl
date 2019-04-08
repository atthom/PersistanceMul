using Distributed
using BenchmarkTools
using IterTools 
@everywhere using StatsBase

@everywhere red_digits(a, b) = BigInt(a*10 + b)

@everywhere function lil_reduce(a, b)
    if a[2] == b[2]
        return (min(a[1], b[1]), a[2]) 
    else
        return a[2] > b[2] ? a : b
    end
end

@everywhere function per(n, level=0)
    if n < 10
        return level
    else
        return per(reduce(*, digits(n)), level + 1)
    end
end

function perform()
    @distributed (lil_reduce) for n in 1:100000000
        (n, per(n))
    end
end

chunk(n, soft_limit) = [repeat([BigInt(n)], i) for i in 0:soft_limit]

function gen_good_candidates(limit)
    case1 = Iterators.product(chunk(2, limit), chunk(3, limit), chunk(7, limit))
    case1 = collect(case1)

    case2 = Iterators.product(chunk(3, limit), chunk(5, limit), chunk(7, limit))
    case2 = collect(case2)
    
    return case1#vcat(case1, case2)
end


repeat_bigint(int, limit) = [repeat([BigInt(int)], limit)]

function gen_new_candidates(limit)
    case1_1 = Iterators.product(repeat_bigint(2, limit), chunk(3, limit-1), chunk(7, limit-1))
    case1_2 = Iterators.product(chunk(2, limit-1), repeat_bigint(3, limit), chunk(7, limit-1))
    case1_3 = Iterators.product(chunk(2, limit-1), chunk(3, limit-1), repeat_bigint(7, limit))
    
    case2_1 = Iterators.product(repeat_bigint(3, limit), chunk(5, limit-1), chunk(7, limit-1))
    case2_2 = Iterators.product(chunk(3, limit-1), repeat_bigint(5, limit), chunk(7, limit-1))
    case2_3 = Iterators.product(chunk(3, limit-1), chunk(5, limit-1), repeat_bigint(7, limit))

    all_case = vcat(case1_1, case1_2, case1_3, case2_1, case2_2, case2_3)
    return vcat([vcat(collect(case)...) for case in all_case]...)
end

@everywhere function persistance(n_digits, level=0)
    if length(n_digits) < 2
        return level
    else
        return persistance(digits(reduce(*, n_digits)), level + 1)
    end
end

@everywhere function repack(n)
    count = StatsBase.countmap(digits(n))
    if !haskey(count, 2)
        count[2] = 0
    end
    if !haskey(count, 3)
        count[3] = 0
    end
    
    count[8] = div(count[2], 3)
    count[2] = rem(count[2], 3)

    count[9] = div(count[3], 2)
    count[3] = rem(count[3], 2)

    count[6] = min(count[2], count[3])
    count[2] = count[2] - count[6]
    count[3] = count[3] - count[6]

    count[4] = div(count[2], 2)
    count[2] = rem(count[2], 2)

    packed = [repeat([k], count[k]) for k in keys(count)]
    return reduce(red_digits, sort(vcat(packed...)))
end

@everywhere function lil_step(a, b, n)
    if a[2] == b[2]
        return (min(a[1], b[1]), a[2]) 
    else
        return a[2] == n ? a : b
    end
end

function hunt_for_step(k, soft_limit)
    lil_reduce(a, b) = lil_step(a, b, k)

    @distributed (lil_reduce) for item in gen_new_candidates(soft_limit)
        c_digits = vcat(item...)
        #println(reduce(red_digits, c_digits), persistance(c_digits))
        (repack(reduce(red_digits, c_digits)), persistance(c_digits))
    end
end


function incremental_seach_for_step(k)
    soft_limit = 5
    println("Start search with soft_limit: ", soft_limit)
    result = hunt_for_step(k, soft_limit)
    while k != result[2]
        soft_limit += 1
        println("Didn't find any solution, increasing to: ", soft_limit)
        result = hunt_for_step(k, soft_limit)
    end
    println("Solution: ", result)
end

function printper(n, level = 0)
    if n < 10
        return level
    else
        level += 1
        numb = reduce(*, [BigInt(i) for i in digits(n)])
        println(numb, " ", level)
        return printper(numb, level)
    end
end

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# 277777777777777777777777778888888888888888999999999999999999999999
# test 13 unit 138. not found