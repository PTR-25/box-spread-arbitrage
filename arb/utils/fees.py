"""
Calculates trading fees, currently focusing on USDC linear options.
"""

import logging

logger = logging.getLogger(__name__)

class FeeCalculator:
    """
    Computes execution and settlement fees per USDC linear option leg.
    Based on Deribit documentation as of early 2024. Fee structures can change.

    Note: For USDC options, combo discounts are typically not applied.
    """

    # Fee Rates (example values, verify with current Deribit fee schedule)
    USDC_EXECUTION_FEE_RATE_INDEX = 0.0005 # 0.05% of index price
    USDC_SETTLEMENT_FEE_RATE_INDEX = 0.00015 # 0.015% of index price (if held to expiry)
    USDC_PREMIUM_FEE_CAP_RATE = 0.125 # 12.5% of premium

    @staticmethod
    def compute_usdc_linear_option_taker_fee_leg(
            premium_per_unit: float,
            index_price: float,
            contract_size: float, # Contract multiplier
            hold_to_expiry: bool = True) -> float:
        """
        Computes the estimated taker fee for a single USDC linear option *contract*.

        Args:
            premium_per_unit (float): The premium of the option leg *per underlying unit* (in USDC).
            index_price (float): The current underlying index price (in USDC).
            contract_size (float): The contract multiplier (units per contract).
            hold_to_expiry (bool): If True, includes the potential settlement fee.

        Returns:
            float: The estimated total fee PER CONTRACT (execution + optional settlement) in USDC.
        """
        if premium_per_unit < 0 or index_price <= 0 or contract_size <= 0:
             logger.warning(f"Invalid input for fee calc: prem={premium_per_unit}, idx={index_price}, cs={contract_size}. Ret 0.")
             return 0.0

        # Calculate total values PER CONTRACT
        value_per_contract_index = index_price * contract_size
        value_per_contract_premium = premium_per_unit * contract_size

        # Ensure premium value isn't negative for cap calculation
        if value_per_contract_premium < 0:
            # This might happen if premium_per_unit is negative (e.g., bid on far OTM put?)
            # Deribit likely floors the cap basis at 0, but using abs() might be safer
            # For simplicity, let's assume premium value used for cap is non-negative
            # Or cap doesn't apply if premium is negative? Let's floor at 0.
            value_per_contract_premium_for_cap = max(0.0, value_per_contract_premium)
            logger.debug(f"Negative premium value ({value_per_contract_premium:.4f}) encountered, using 0 for fee cap basis.")
        else:
            value_per_contract_premium_for_cap = value_per_contract_premium


        # --- Execution Fee per contract ---
        # Fee based on index value: Rate * IndexValuePerContract
        fee_based_on_index = FeeCalculator.USDC_EXECUTION_FEE_RATE_INDEX * value_per_contract_index
        # Fee cap based on premium value: CapRate * PremiumValuePerContract
        fee_cap_based_on_premium = FeeCalculator.USDC_PREMIUM_FEE_CAP_RATE * value_per_contract_premium_for_cap
        # Execution fee is the minimum of the two
        execution_fee = min(fee_based_on_index, fee_cap_based_on_premium)

        # --- Settlement Fee per contract (potential) ---
        settlement_fee = 0.0
        if hold_to_expiry:
            # Calculated similarly to execution fee but with settlement rate
            settle_fee_based_on_index = FeeCalculator.USDC_SETTLEMENT_FEE_RATE_INDEX * value_per_contract_index
            # Settlement cap uses the same premium value cap basis
            settle_fee_cap = FeeCalculator.USDC_PREMIUM_FEE_CAP_RATE * value_per_contract_premium_for_cap
            settlement_fee = min(settle_fee_based_on_index, settle_fee_cap)

        total_fee_per_contract = execution_fee + settlement_fee
        logger.debug(
            f"Fee Calc (Per Contract): PremUnit={premium_per_unit:.4f}, Idx={index_price:.2f}, CS={contract_size}, Hold={hold_to_expiry} -> "
            f"ExecFee={execution_fee:.4f}, SettleFee={settlement_fee:.4f}, TotalFee={total_fee_per_contract:.4f}"
        )

        return total_fee_per_contract

    @staticmethod
    def apply_combo_discount(fee_buy: float, fee_sell: float) -> float:
        """
        Applies combo discounts if applicable. Currently assumes no discount for USDC options.

        Args:
            fee_buy (float): Calculated fee for the buy leg.
            fee_sell (float): Calculated fee for the sell leg.

        Returns:
            float: The total fee after considering discounts (currently just sum).
        """
        # For USDC options, Deribit documentation usually states no combo discount.
        # For inverse options (BTC/ETH), combo discounts might apply differently.
        total_fee = fee_buy + fee_sell
        logger.debug(f"Applying combo discount (none for USDC): BuyFee={fee_buy:.4f}, SellFee={fee_sell:.4f} -> Total={total_fee:.4f}")
        return total_fee