####
## Magpie Technical Test Answer - Data Engineer
## by Mario Caesar // caesarmario87@gmail.com
####

def find_discount(prices_value):
    try:
        # Missing space handler
        if " " not in prices_value:
            raise ValueError("Entered value should contain space between each price!")
        
        prices = []
        for value in prices_value.split():
            try:
                prices.append(float(value))  # Attempt to convert each value to float
            except ValueError:
                raise ValueError(f"This is not an int or float!!! '{value}'")

        # Drop index list
        drop_index = []

        # Find idx reduction using iteration
        for idx in range(1, len(prices)):
            # if current price < prev price, append idx where reduction happened
            if prices[idx] < prices[idx-1]:
                drop_index.append(idx)

        # if no reduction, return 0
        return drop_index if drop_index else [0]

    except ValueError as e:
        print(f"--- Error detected: {e}")
        return None

### Test cases ###
# print(find_discount(prices_value="3 3 3 3"))
# print(find_discount(prices_value="3 4 3"))
# print(find_discount(prices_value="3 4 1"))
# print(find_discount(prices_value="1 0 3"))
# print(find_discount(prices_value="1 3 3"))
# print(find_discount(prices_value="100 150 90 90 80 70 60 50 40 30 20 10 0 1 10000 500 432 432 432.5 12356 50000"))
# print(find_discount(prices_value="100 200 150 300 250 500000 4312 5143 123 100 ioo"))
# print(find_discount(prices_value="432.5 123 784"))