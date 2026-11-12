export type DestinationDto = {
  id: string;
  code: string;
};

export type DestinationListDto = {
  items: DestinationDto[];
};

export type DeliveryListDto = {
  id: string;
  destination_code: string;
  status: string;
  has_unconfirmed_attempts: boolean;
};

export type EventListItemDto = {
  id: string;
  type: string;
  created_at: string;
  deliveries: DeliveryListDto[];
};

export type EventListDto = {
  items: EventListItemDto[];
  next_cursor: string | null;
};

export type AttemptDto = {
  id: string;
  attempt_no: number;
  cycle_no: number;
  cycle_attempt_no: number;
  outcome: string;
  started_at: string;
  finished_at: string | null;
  http_status: number | null;
  error_code: string | null;
  duration_ms: number | null;
  retry_delay_ms: number | null;
};

export type ReplayDto = {
  id: string;
  from_cycle: number;
  to_cycle: number;
  actor: string;
  created_at: string;
};

export type EventDeliveryDetailDto = DeliveryListDto & {
  operation_id: string;
  cycle_no: number;
  cycle_attempt_count: number;
  lifetime_attempt_count: number;
  max_attempts: number;
  next_attempt_at: string | null;
  lease_until: string | null;
  confirmed_at: string | null;
  terminal_reason: string | null;
  attempts: AttemptDto[];
  replays: ReplayDto[];
};

export type EventDetailDto = {
  id: string;
  type: string;
  created_at: string;
  payload: unknown;
  deliveries: EventDeliveryDetailDto[];
};

export type EventCreateResponseDto = {
  event_id: string;
  deduplicated: boolean;
  created_at: string;
  delivery_ids: string[];
};

export type ReplayResponseDto = {
  replay_id: string;
  delivery_id: string;
  cycle_no: number;
  deduplicated: boolean;
};
