/**
 * Additional context that is stable and should be included in every call.
 */
export function buildContext({ rebalanceTimeLocal }) {
  return {
    constraints: {
      rebalance_time: rebalanceTimeLocal,
      holding_period: "Hold position until next day's rebalance time.",
      allowed_positions: ["LONG_ASSET", "LONG_CASH", "SHORT_ASSET"],
      leverage: "none",
      shorting: "allowed (no leverage)",
      cash_return: 0,
      transaction_costs: 0
    },
    // Kept for model decision-making, but the model is instructed not to mention this in the explanation.
    objective: "Maximize long-term Sharpe ratio (return/volatility) of a daily strategy."
  };
}
